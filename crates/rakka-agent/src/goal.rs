//! The goal contract and its lifecycle.
//!
//! Owns both halves of the contract of
//! [specification 8.1](../../../docs/plans/rakka-agent/spec.md). The mode half:
//! [`AgentGoalMode`] distinguishes a finite goal, which terminates once its
//! criteria are evaluated, from a continuous one, which executes as bounded
//! durable epochs admitted by the wake controller of [`crate::wake`] — never as
//! an immortal polling future. The contract half, landed by slice 4.1:
//! [`AgentGoalSpec`] is the durable, versioned goal contract — owner,
//! objective, success criteria, budgets, allowed references, evaluator,
//! escalation — and [`AgentGoalState`] carries its [`AgentGoalStatus`]
//! lifecycle with the `Unsatisfied`/`Failed` distinction.
//!
//! The goal deliberately has no entity of its own
//! ([specification 6.3](../../../docs/plans/rakka-agent/spec.md)): the root
//! `AgentTaskEntity` coordinates it, holding the goal record as a component of
//! its own durable state so every goal transition commits in the same
//! compare-and-set as the task transition that decided it. The goal identity
//! defaults to the root task's value
//! ([`AgentGoalId::for_root_task`](crate::identity::AgentGoalId::for_root_task))
//! while the two types stay distinct, and a goal stays addressable while fully
//! passivated. Identity therefore lives on the coordinating record, not here:
//! the tenant, goal id, root task id, mode, and current coordinator run of
//! specification 8.1's field list are the root task's scope, `goal`,
//! `goal_mode`, and assignment fields, composed around this spec rather than
//! duplicated inside it.
//!
//! [`AgentGoalStatus`] is the goal-*contract* status. It is deliberately
//! distinct from the continuous-goal *admission gate*
//! [`crate::wake::AgentGoalLifecycleStatus`], which governs whether the wake
//! controller admits epochs; gate transitions project one-way onto the goal
//! record where the two overlap (an observed expiry, a retirement, a
//! suspension), never the reverse.
//!
//! Specification: sections 8.1 and 6.3, with the continuous clauses of 8.2 and
//! the goal-scope budget rules of 9.7.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use rakka_agent_workflow::{AgentTimestampMillis, ArtifactRef, PrincipalRef, StateSchemaVersion};
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::budget::{
    AgentBudgetAllocation, AgentBudgetDimension, AgentBudgetExhaustion, AgentBudgetLimits,
};
use crate::definition::{
    AgentCapabilityId, AgentPolicyRef, AgentRevisionNumber, AgentRevisionProvenance, AgentToolId,
    AgentWorkflowToolId,
};
use crate::evaluation::{
    AgentGoalEvaluationMethodKind, AgentGoalEvidenceRef, AgentGoalStagnationAction,
    AgentGoalStagnationPolicy, AgentStagnationTrigger, AGENT_GOAL_EVALUATION_MAX_EVIDENCE,
};
use crate::identity::{AgentEnvironmentRef, AgentId, AgentRunId, KnowledgeSpaceId};
use crate::schema::{
    AgentRecordKind, VersionedAgentRecord, CURRENT_AGENT_GOAL_SPEC_SCHEMA_VERSION,
};
use crate::task::{AgentContentDigest, AgentTaskDefinition};
use crate::wake::{AgentWakePolicyRevision, ScheduleRevision};

/// Result type for goal contract construction and transitions.
pub type AgentGoalResult<T> = Result<T, AgentGoalError>;

/// Maximum serialized size, in bytes, of one [`AgentGoalSpec`].
///
/// The spec is a component of the root task's bounded materialized record, so
/// its own bound is what keeps a goal-bearing task inside
/// [`crate::task::AGENT_TASK_MATERIALIZED_MAX_BYTES`] with the growth reserve
/// intact. Objectives and criteria larger than this belong in the referenced
/// artifact, not the spec.
pub const AGENT_GOAL_SPEC_MAX_BYTES: usize = 4 * 1024;

/// Maximum length, in bytes, of the bounded objective summary.
pub const AGENT_GOAL_SUMMARY_MAX_LENGTH: usize = 1024;

/// Maximum entries in each of the spec's allowed-reference sets.
pub const AGENT_GOAL_MAX_ALLOWED_REFS: usize = 32;

/// Maximum length, in bytes, of one required-evidence class label.
pub const AGENT_GOAL_EVIDENCE_CLASS_MAX_LENGTH: usize = 128;

/// Maximum length, in bytes, of a bounded reason or code string carried by a
/// goal decision.
///
/// [`AgentGoalState::decide`] truncates a decision's string payloads to this
/// bound before persisting them — the operator-reason idiom the wake
/// lifecycle and task cancellation already follow — so a caller-supplied
/// reason can never grow the durable record unboundedly.
pub const AGENT_GOAL_REASON_MAX_LENGTH: usize = 256;

/// Truncates a decision reason to its bound on a character boundary.
pub(crate) fn bounded_reason_text(mut value: String) -> String {
    if value.len() > AGENT_GOAL_REASON_MAX_LENGTH {
        let mut end = AGENT_GOAL_REASON_MAX_LENGTH;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        value.truncate(end);
    }
    value
}

/// Whether a goal terminates after one evaluation of its criteria or operates
/// as bounded durable epochs
/// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// The default is finite: continuous operation is always an explicit
/// declaration, because it is what autonomy admission gates and what the wake
/// controller schedules.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGoalMode {
    /// The goal terminates after its current versioned criteria are evaluated.
    #[default]
    Finite,
    /// The goal executes as bounded durable epochs admitted from deduplicated
    /// wake occurrences.
    Continuous(Box<AgentContinuousGoalSpec>),
}

impl AgentGoalMode {
    /// Whether the goal operates continuously.
    #[must_use]
    pub const fn is_continuous(&self) -> bool {
        matches!(self, Self::Continuous(_))
    }

    /// The continuous specification, when the goal has one.
    #[must_use]
    pub const fn continuous(&self) -> Option<&AgentContinuousGoalSpec> {
        match self {
            Self::Finite => None,
            Self::Continuous(spec) => Some(spec),
        }
    }
}

/// What the continuous-goal controller needs from the goal contract
/// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md) continuous
/// clauses, [8.2](../../../docs/plans/rakka-agent/spec.md)).
///
/// This is deliberately the controller's slice of the contract, not the full
/// goal specification: objective, success criteria, evaluator, and escalation
/// live on [`AgentGoalSpec`] and sit *around* these fields, not inside them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentContinuousGoalSpec {
    /// The schedule revision currently in force. A schedule update advances it
    /// and fences pending wakes constructed under an obsolete one.
    pub schedule_revision: ScheduleRevision,
    /// The versioned wake policy: triggers, overlap, coalescing,
    /// missed-occurrence, per-epoch budget and deadline, window ceiling,
    /// backoff, and lifecycle.
    pub wake_policy: AgentWakePolicyRevision,
    /// The explicit health condition unattended operation is checked against.
    /// The reference is application-owned; the controller records which
    /// condition governed each renewal or retirement decision.
    pub health_condition: AgentPolicyRef,
    /// The epoch contract: what each admitted occurrence executes. Records
    /// persisted before this field load without one, and the controller then
    /// refuses epoch admission closed rather than guessing a definition.
    #[serde(default)]
    pub epoch: Option<Box<AgentEpochSpec>>,
}

/// The epoch contract of one continuous goal
/// ([specification 8.2](../../../docs/plans/rakka-agent/spec.md)): the typed
/// work each admitted occurrence executes as a finite child task and run.
///
/// The definition is the epoch's result/evidence contract, and it is
/// deliberately distinct from the root control task's own definition: the
/// root coordinates, the epoch works. The observation scope is the authorized
/// input reference each epoch observes; being an [`AgentEnvironmentRef`], it
/// can never carry resolved credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEpochSpec {
    /// The typed task definition every epoch runs under.
    pub definition: AgentTaskDefinition,
    /// The agent every epoch is assigned to.
    pub assignee: AgentId,
    /// The authorized observation scope each epoch's input references, when
    /// the goal observes one.
    pub observation_scope: Option<AgentEnvironmentRef>,
}

/// The goal-contract lifecycle status
/// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// `Satisfied`, `Unsatisfied`, `Failed`, `Cancelled`, and `Expired` are
/// terminal and absorbing. `Unsatisfied` records an evaluator or policy
/// decision that the success criteria were not met under the current goal
/// revision; `Failed` records an execution or policy failure that ended the
/// goal. Every terminal status is reachable from both `Active` and `Waiting`.
///
/// This is the goal's *contract* status, not a residency claim: a goal in any
/// of these states is fully passivatable, and it is deliberately distinct from
/// the continuous-goal admission gate
/// [`crate::wake::AgentGoalLifecycleStatus`], whose `Active`/`Expired` labels
/// name whether the wake controller admits epochs, not whether the goal's
/// criteria were met.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGoalStatus {
    /// Instituted but not yet authorized to spend anything.
    Proposed,
    /// Authorized: the coordinator works toward the criteria.
    Active,
    /// Parked behind a structured [`AgentGoalWaitReason`]; nothing spends
    /// until an authorized resume.
    Waiting,
    /// Terminal: the configured evaluator accepted the current criteria
    /// revision against durable evidence.
    Satisfied,
    /// Terminal: an evaluator or policy decided the criteria were not met
    /// under the current goal revision.
    Unsatisfied,
    /// Terminal: an execution or policy failure ended the goal.
    Failed,
    /// Terminal: the goal was cancelled or retired by an authorized decision.
    Cancelled,
    /// Terminal: the goal's deadline or schedule expiry passed.
    Expired,
}

impl AgentGoalStatus {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Proposed => "proposed",
            Self::Active => "active",
            Self::Waiting => "waiting",
            Self::Satisfied => "satisfied",
            Self::Unsatisfied => "unsatisfied",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }

    /// Whether the status is terminal and absorbing.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Satisfied | Self::Unsatisfied | Self::Failed | Self::Cancelled | Self::Expired
        )
    }

    /// Whether the goal authorizes work to be spent on it right now.
    #[must_use]
    pub const fn permits_work(self) -> bool {
        matches!(self, Self::Active)
    }
}

impl Display for AgentGoalStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_label())
    }
}

/// Why a goal reached a terminal status.
///
/// The reason determines the terminal [`AgentGoalStatus`] through
/// [`Self::status`], following the task's terminal-reason precedent: the pair
/// is derived, never stored separately, so an inconsistent outcome/reason
/// combination is unrepresentable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGoalTerminalReason {
    /// The configured evaluator accepted the current criteria revision against
    /// durable evidence.
    CriteriaSatisfied,
    /// An evaluator or policy decided the criteria were not met under the
    /// current goal revision.
    CriteriaNotMet,
    /// An execution or policy failure ended the goal.
    ExecutionFailed {
        /// Bounded, stable failure code.
        code: String,
    },
    /// The goal-scope budget was exhausted under a `Terminate` exhaustion
    /// policy ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    BudgetExhausted {
        /// The ceiling that was reached.
        exhaustion: AgentBudgetExhaustion,
    },
    /// The goal was cancelled by an authorized command.
    CancellationRequested {
        /// Bounded, stable reason.
        reason: String,
    },
    /// The goal's root task was cancelled, taking the goal with it.
    RootTaskCancelled,
    /// The continuous goal was retired by command or by its retirement policy.
    ///
    /// Specification 8.1's status set has no `Retired`: retirement is an
    /// authorized stop that is neither an evaluation outcome, a failure, nor
    /// an expiry, so it terminates the goal `Cancelled` with this reason on
    /// record — a structured reason rather than a new top-level status
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md)).
    Retired,
    /// The goal's own deadline passed.
    DeadlineExpired,
    /// The continuous goal's schedule expiry passed without the renewal its
    /// policy required.
    ScheduleExpired,
    /// A stagnation threshold tripped under a `Terminate` action
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
    ///
    /// A policy decision, so the goal ends `Failed` — never `Unsatisfied`,
    /// which stays unconstructible without an evaluation of the criteria.
    Stagnant {
        /// The condition that tripped.
        trigger: AgentStagnationTrigger,
        /// The streak length at the trip.
        epochs: u32,
    },
}

impl AgentGoalTerminalReason {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::CriteriaSatisfied => "criteria-satisfied",
            Self::CriteriaNotMet => "criteria-not-met",
            Self::ExecutionFailed { .. } => "execution-failed",
            Self::BudgetExhausted { .. } => "budget-exhausted",
            Self::CancellationRequested { .. } => "cancellation-requested",
            Self::RootTaskCancelled => "root-task-cancelled",
            Self::Retired => "retired",
            Self::DeadlineExpired => "deadline-expired",
            Self::ScheduleExpired => "schedule-expired",
            Self::Stagnant { .. } => "stagnant",
        }
    }

    /// The terminal status this reason ends the goal in.
    #[must_use]
    pub const fn status(&self) -> AgentGoalStatus {
        match self {
            Self::CriteriaSatisfied => AgentGoalStatus::Satisfied,
            Self::CriteriaNotMet => AgentGoalStatus::Unsatisfied,
            Self::ExecutionFailed { .. } | Self::BudgetExhausted { .. } | Self::Stagnant { .. } => {
                AgentGoalStatus::Failed
            }
            Self::CancellationRequested { .. } | Self::RootTaskCancelled | Self::Retired => {
                AgentGoalStatus::Cancelled
            }
            Self::DeadlineExpired | Self::ScheduleExpired => AgentGoalStatus::Expired,
        }
    }

    /// Whether this reason records a criteria decision, which only an
    /// evaluation can make
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
    #[must_use]
    pub const fn requires_evaluation(&self) -> bool {
        matches!(self, Self::CriteriaSatisfied | Self::CriteriaNotMet)
    }
}

/// Why a goal is `Waiting`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGoalWaitReason {
    /// The goal-scope budget is exhausted under a `Park` exhaustion policy;
    /// an authorized resume, optionally with a top-up, reactivates.
    BudgetExhausted {
        /// The ceiling that was reached.
        exhaustion: AgentBudgetExhaustion,
    },
    /// The goal-scope budget is exhausted under an `Escalate` exhaustion
    /// policy: parked exactly as `BudgetExhausted`, with the escalation
    /// recorded against the spec's escalation policy reference.
    Escalated {
        /// The ceiling that was reached.
        exhaustion: AgentBudgetExhaustion,
    },
    /// The continuous goal's admission gate was suspended; the goal contract
    /// waits until an authorized resume lifts the suspension.
    AdmissionSuspended,
    /// A stagnation threshold tripped under a `Wait` or `Escalate` action;
    /// an authorized `ResumeGoal` reactivates and resets the detector.
    ///
    /// Deliberately no streak count in the payload: a raced epoch settlement
    /// after the park re-parks on the identical reason as a no-op instead of
    /// burning a status revision per late settle. The streak lives in the wake
    /// controller's durable counters and the history detail.
    Stagnant {
        /// The condition that tripped.
        trigger: AgentStagnationTrigger,
    },
}

impl AgentGoalWaitReason {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::BudgetExhausted { .. } => "budget-exhausted",
            Self::Escalated { .. } => "escalated",
            Self::AdmissionSuspended => "admission-suspended",
            Self::Stagnant { .. } => "stagnant",
        }
    }

    /// The budget exhaustion this wait records, when it records one.
    #[must_use]
    pub const fn exhaustion(&self) -> Option<&AgentBudgetExhaustion> {
        match self {
            Self::BudgetExhausted { exhaustion } | Self::Escalated { exhaustion } => {
                Some(exhaustion)
            }
            Self::AdmissionSuspended | Self::Stagnant { .. } => None,
        }
    }
}

/// What a goal-scope budget exhaustion does
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md): hard
/// ceilings deterministically park, escalate, or terminate per persisted
/// policy).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGoalExhaustionAction {
    /// Park the goal `Waiting` with the structured exhaustion on record; for
    /// a continuous goal, suspend epoch admission in the same transition so
    /// triggers coalesce and nothing spends.
    #[default]
    Park,
    /// Park exactly as `Park`, and record the escalation against the spec's
    /// escalation policy reference. Goal-scope HITL wiring is a later slice;
    /// until it lands, escalation is the durable record plus the park.
    Escalate,
    /// Terminate the goal `Failed` with the exhaustion as its terminal
    /// reason, and the root task with it.
    Terminate,
}

impl AgentGoalExhaustionAction {
    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Park => "park",
            Self::Escalate => "escalate",
            Self::Terminate => "terminate",
        }
    }
}

/// The goal-scope budget-exhaustion policy
/// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md),
/// [9.7](../../../docs/plans/rakka-agent/spec.md)).
///
/// It governs *allocation* exhaustion — the escrowed goal budget the root task
/// holds. The continuous goal-window ceiling is deliberately outside it: a
/// window wait is self-relieving at the persisted window turn, so consulting a
/// park/escalate policy there would demand an operator resume for a condition
/// that resolves itself.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalExhaustionPolicy {
    /// The action taken when no per-dimension override applies.
    #[serde(default)]
    pub default: AgentGoalExhaustionAction,
    /// Per-dimension overrides.
    #[serde(default)]
    pub overrides: BTreeMap<AgentBudgetDimension, AgentGoalExhaustionAction>,
}

impl AgentGoalExhaustionPolicy {
    /// The action this policy takes for an exhaustion in `dimension`.
    #[must_use]
    pub fn action_for(&self, dimension: AgentBudgetDimension) -> AgentGoalExhaustionAction {
        self.overrides
            .get(&dimension)
            .copied()
            .unwrap_or(self.default)
    }
}

/// The goal's objective: an artifact reference plus a bounded summary.
///
/// The full objective belongs in the referenced artifact; the summary is what
/// stays inside the bounded durable record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalObjective {
    /// The objective artifact, when one exists out of line.
    #[serde(default)]
    pub artifact: Option<ArtifactRef>,
    /// Bounded summary of the objective.
    pub summary: String,
}

/// Where the goal's success criteria live.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentGoalCriteriaSource {
    /// The criteria are an out-of-line artifact.
    Artifact(Box<ArtifactRef>),
    /// The criteria are an application-owned policy.
    Policy(AgentPolicyRef),
}

/// The versioned success criteria of one goal
/// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// The revision is what a completion evaluation binds: slice 4.2's evaluator
/// assesses *this* revision against durable evidence, and a decision carrying
/// a stale revision is refused
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalCriteria {
    /// Where the criteria live.
    pub source: AgentGoalCriteriaSource,
    /// The criteria revision currently in force.
    pub revision: AgentRevisionNumber,
    /// Content digest of the criteria, when the application fingerprints them.
    #[serde(default)]
    pub digest: Option<AgentContentDigest>,
}

/// The delegation ceilings of one goal
/// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md) descendant
/// dimensions). `None` means unbounded.
///
/// Enforced at the delegation door (slice 4.4): depth, fan-out, and
/// concurrency are checked against the run's envelope and durable delegation
/// cells before a delegation commits, and the descendants ceiling seeds the
/// conserved [`AgentBudgetDimension::Descendants`](crate::AgentBudgetDimension)
/// dimension of the root task's escrow ledger. Every refusal is a failed tool
/// result the model corrects course from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalDelegationBudget {
    /// Maximum delegation depth below the root.
    #[serde(default)]
    pub max_depth: Option<u32>,
    /// Maximum direct children of one delegating run.
    #[serde(default)]
    pub max_fan_out: Option<u32>,
    /// Maximum descendants across the whole delegation graph.
    #[serde(default)]
    pub max_descendants: Option<u32>,
    /// Maximum concurrently unsettled direct children of one delegating run.
    ///
    /// Per-run and direct, not tree-global: a tree-wide *level* cannot be
    /// enforced by single-entity transitions
    /// ([specification 9.7](../../../docs/plans/rakka-agent/spec.md) forbids
    /// dispatch-time cross-entity reads), and tree-wide *mass* is what
    /// `max_descendants` bounds. A child counts until its send settles
    /// definitively or its terminal result is recorded.
    #[serde(default)]
    pub max_concurrent: Option<u32>,
}

impl AgentGoalDelegationBudget {
    /// These ceilings narrowed to `ceiling`: the smaller of the two per field,
    /// with `None` as the unbounded identity — exactly
    /// [`AgentBudgetAllocation::narrowed_to`]'s rule, so a child's delegation
    /// authority can never widen what its parent or definition permits.
    #[must_use]
    pub fn narrowed_to(&self, ceiling: &Self) -> Self {
        fn tighter(held: Option<u32>, bound: Option<u32>) -> Option<u32> {
            match (held, bound) {
                (None, bound) => bound,
                (held, None) => held,
                (Some(held), Some(bound)) => Some(held.min(bound)),
            }
        }
        Self {
            max_depth: tighter(self.max_depth, ceiling.max_depth),
            max_fan_out: tighter(self.max_fan_out, ceiling.max_fan_out),
            max_descendants: tighter(self.max_descendants, ceiling.max_descendants),
            max_concurrent: tighter(self.max_concurrent, ceiling.max_concurrent),
        }
    }
}

/// The durable, versioned goal contract
/// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// Identity is composed, not duplicated: the tenant, goal id, root task id,
/// goal mode (with the continuous wake/overlap/missed/suspension/retirement
/// policy), and current coordinator run live on the root task record that
/// holds this spec, so the one compare-and-set that moves the task also moves
/// the goal.
///
/// The allowed-reference sets narrow what the goal may use *within* what the
/// agent definitions involved already authorize; an empty set means no
/// goal-level narrowing. They are typed but inert until the slice that owns
/// each flow enforces them: skills and tools in 4.3, workflows in 4.5,
/// knowledge spaces and environments in 4.6. The evaluator and
/// required-evidence references are enforced at the decision door, and the
/// stagnation thresholds bound the wake controller's detector (slice 4.2);
/// the terminal-decision reference stays recorded-only. The escalation
/// reference is recorded by the `Escalate` exhaustion and stagnation actions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentGoalSpec {
    /// The owner or principal the goal is accountable to.
    pub owner: PrincipalRef,
    /// The objective: artifact reference plus bounded summary.
    pub objective: AgentGoalObjective,
    /// The versioned success criteria.
    pub criteria: AgentGoalCriteria,
    /// Priority relative to the owner's other goals; higher is more urgent.
    #[serde(default)]
    pub priority: Option<u32>,
    /// The goal's own deadline. Observed opportunistically at every goal
    /// entry point: a passed deadline expires the goal.
    #[serde(default)]
    pub deadline: Option<AgentTimestampMillis>,
    /// The cancellation policy reference, consumed by slice 4.6's propagation.
    #[serde(default)]
    pub cancellation: Option<AgentPolicyRef>,
    /// The conserved goal allocation. It seeds the root task's escrow ledger,
    /// narrowed to the definition ceilings — the definition-ceiling → goal →
    /// task rung of [specification 9.7](../../../docs/plans/rakka-agent/spec.md).
    #[serde(default)]
    pub allocation: AgentBudgetAllocation,
    /// The non-conserved limits the goal runs under.
    #[serde(default)]
    pub limits: AgentBudgetLimits,
    /// Delegation ceilings, enforced at the delegation door (slice 4.4).
    #[serde(default)]
    pub delegation: Option<AgentGoalDelegationBudget>,
    /// The fan-in policy this goal's fan-out groups open under, when the
    /// goal declares one; the run wiring's default applies otherwise. Fixed
    /// in durable state at group open — the model chooses when to await,
    /// never what the rule is
    /// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
    #[serde(default)]
    pub fan_in: Option<crate::fan_in::AgentFanInPolicy>,
    /// What a goal-scope budget exhaustion does. Active in this slice.
    #[serde(default)]
    pub exhaustion: AgentGoalExhaustionPolicy,
    /// Agent skills the goal may delegate to (slice 4.3).
    #[serde(default)]
    pub allowed_skills: BTreeSet<AgentCapabilityId>,
    /// Tools the goal may use (slice 4.3).
    #[serde(default)]
    pub allowed_tools: BTreeSet<AgentToolId>,
    /// Compiled workflows the goal may invoke as tools (slice 4.5).
    #[serde(default)]
    pub allowed_workflows: BTreeSet<AgentWorkflowToolId>,
    /// Knowledge spaces the goal may read or contribute to (slice 4.6).
    #[serde(default)]
    pub knowledge_spaces: BTreeSet<KnowledgeSpaceId>,
    /// Shared environments the goal may reach (slice 4.6).
    #[serde(default)]
    pub environments: BTreeSet<AgentEnvironmentRef>,
    /// The completion evaluator reference (slice 4.2).
    #[serde(default)]
    pub evaluator: Option<AgentPolicyRef>,
    /// Evidence classes a completion evaluation must present (slice 4.2).
    #[serde(default)]
    pub required_evidence: BTreeSet<String>,
    /// The escalation policy reference an `Escalate` exhaustion records.
    #[serde(default)]
    pub escalation: Option<AgentPolicyRef>,
    /// The terminal-decision policy reference (slice 4.2).
    #[serde(default)]
    pub terminal_decision: Option<AgentPolicyRef>,
    /// The application-owned stagnation policy reference, recorded when a
    /// stagnation action fires.
    #[serde(default)]
    pub stagnation: Option<AgentPolicyRef>,
    /// The deterministic stagnation thresholds and actions the wake
    /// controller's detector enforces. Detection is disabled unless a
    /// threshold is set.
    #[serde(default)]
    pub stagnation_policy: AgentGoalStagnationPolicy,
    /// The settings revision the goal was accepted under, when pinned.
    #[serde(default)]
    pub settings_revision: Option<AgentRevisionNumber>,
    /// The policy revision the goal was accepted under, when pinned.
    #[serde(default)]
    pub policy_revision: Option<AgentRevisionNumber>,
}

impl AgentGoalSpec {
    /// Rejects a spec that violates a bounded invariant.
    ///
    /// Runs where a spec enters — [`AgentGoalSpecRevision::initial`],
    /// [`AgentGoalSpecRevision::updated`], and deserialization — so a spec
    /// that violates a bound fails closed everywhere.
    pub fn validate(&self) -> AgentGoalResult<()> {
        if self.objective.summary.len() > AGENT_GOAL_SUMMARY_MAX_LENGTH {
            return Err(AgentGoalError::SummaryTooLong {
                length: self.objective.summary.len(),
                maximum: AGENT_GOAL_SUMMARY_MAX_LENGTH,
            });
        }
        for (field, length) in [
            ("allowed_skills", self.allowed_skills.len()),
            ("allowed_tools", self.allowed_tools.len()),
            ("allowed_workflows", self.allowed_workflows.len()),
            ("knowledge_spaces", self.knowledge_spaces.len()),
            ("environments", self.environments.len()),
            ("required_evidence", self.required_evidence.len()),
        ] {
            if length > AGENT_GOAL_MAX_ALLOWED_REFS {
                return Err(AgentGoalError::CollectionTooLarge {
                    field,
                    length,
                    maximum: AGENT_GOAL_MAX_ALLOWED_REFS,
                });
            }
        }
        if let Some(class) = self
            .required_evidence
            .iter()
            .find(|class| class.len() > AGENT_GOAL_EVIDENCE_CLASS_MAX_LENGTH)
        {
            return Err(AgentGoalError::LabelTooLong {
                field: "required_evidence",
                length: class.len(),
                maximum: AGENT_GOAL_EVIDENCE_CLASS_MAX_LENGTH,
            });
        }
        // A spec that requires more evidence classes than one evaluation may
        // present is statically unsatisfiable, so it is refused at the door
        // it would later dead-lock.
        if self.required_evidence.len() > AGENT_GOAL_EVALUATION_MAX_EVIDENCE {
            return Err(AgentGoalError::CollectionTooLarge {
                field: "required_evidence",
                length: self.required_evidence.len(),
                maximum: AGENT_GOAL_EVALUATION_MAX_EVIDENCE,
            });
        }
        if let Some(threshold) = self.stagnation_policy.repeated_result_epochs {
            if threshold < 2 {
                return Err(AgentGoalError::StagnationThresholdTooLow {
                    field: "repeated_result_epochs",
                    value: threshold,
                    minimum: 2,
                });
            }
        }
        if let Some(threshold) = self.stagnation_policy.no_progress_epochs {
            if threshold < 1 {
                return Err(AgentGoalError::StagnationThresholdTooLow {
                    field: "no_progress_epochs",
                    value: threshold,
                    minimum: 1,
                });
            }
        }
        // Replan is typed for later slices and refused until one can execute
        // it honestly: nothing re-issues an epoch with changed input yet, and
        // mapping it to a park would silently change behavior when real
        // replanning lands.
        if self
            .stagnation_policy
            .selects(AgentGoalStagnationAction::Replan)
        {
            return Err(AgentGoalError::StagnationReplanUnsupported);
        }
        // A quorum that can never resolve is refused at the door it would
        // later dead-lock, exactly as a statically unsatisfiable evidence
        // demand is. The policy owns the rule; this door only translates
        // its refusal into the goal's vocabulary.
        if let Some(policy) = self.fan_in {
            if let Err(crate::delegation::AgentDelegationError::QuorumInvalid { n, maximum }) =
                policy.validate()
            {
                return Err(AgentGoalError::FanInQuorumInvalid { n, maximum });
            }
        }
        let bytes = serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0);
        if bytes > AGENT_GOAL_SPEC_MAX_BYTES {
            return Err(AgentGoalError::SpecTooLarge {
                bytes,
                maximum: AGENT_GOAL_SPEC_MAX_BYTES,
            });
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for AgentGoalSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Record {
            owner: PrincipalRef,
            objective: AgentGoalObjective,
            criteria: AgentGoalCriteria,
            #[serde(default)]
            priority: Option<u32>,
            #[serde(default)]
            deadline: Option<AgentTimestampMillis>,
            #[serde(default)]
            cancellation: Option<AgentPolicyRef>,
            #[serde(default)]
            allocation: AgentBudgetAllocation,
            #[serde(default)]
            limits: AgentBudgetLimits,
            #[serde(default)]
            delegation: Option<AgentGoalDelegationBudget>,
            #[serde(default)]
            fan_in: Option<crate::fan_in::AgentFanInPolicy>,
            #[serde(default)]
            exhaustion: AgentGoalExhaustionPolicy,
            #[serde(default)]
            allowed_skills: BTreeSet<AgentCapabilityId>,
            #[serde(default)]
            allowed_tools: BTreeSet<AgentToolId>,
            #[serde(default)]
            allowed_workflows: BTreeSet<AgentWorkflowToolId>,
            #[serde(default)]
            knowledge_spaces: BTreeSet<KnowledgeSpaceId>,
            #[serde(default)]
            environments: BTreeSet<AgentEnvironmentRef>,
            #[serde(default)]
            evaluator: Option<AgentPolicyRef>,
            #[serde(default)]
            required_evidence: BTreeSet<String>,
            #[serde(default)]
            escalation: Option<AgentPolicyRef>,
            #[serde(default)]
            terminal_decision: Option<AgentPolicyRef>,
            #[serde(default)]
            stagnation: Option<AgentPolicyRef>,
            #[serde(default)]
            stagnation_policy: AgentGoalStagnationPolicy,
            #[serde(default)]
            settings_revision: Option<AgentRevisionNumber>,
            #[serde(default)]
            policy_revision: Option<AgentRevisionNumber>,
        }

        let record = Record::deserialize(deserializer)?;
        let spec = Self {
            owner: record.owner,
            objective: record.objective,
            criteria: record.criteria,
            priority: record.priority,
            deadline: record.deadline,
            cancellation: record.cancellation,
            allocation: record.allocation,
            limits: record.limits,
            delegation: record.delegation,
            fan_in: record.fan_in,
            exhaustion: record.exhaustion,
            allowed_skills: record.allowed_skills,
            allowed_tools: record.allowed_tools,
            allowed_workflows: record.allowed_workflows,
            knowledge_spaces: record.knowledge_spaces,
            environments: record.environments,
            evaluator: record.evaluator,
            required_evidence: record.required_evidence,
            escalation: record.escalation,
            terminal_decision: record.terminal_decision,
            stagnation: record.stagnation,
            stagnation_policy: record.stagnation_policy,
            settings_revision: record.settings_revision,
            policy_revision: record.policy_revision,
        };
        spec.validate().map_err(DeserializeError::custom)?;
        Ok(spec)
    }
}

/// One accepted revision of a goal's spec
/// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// The spec is versioned exactly as the wake policy is: the revision names
/// which contract a decision was made under, and the schema version fails
/// closed on a record a newer writer persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalSpecRevision {
    schema_version: StateSchemaVersion,
    revision: AgentRevisionNumber,
    spec: AgentGoalSpec,
    provenance: AgentRevisionProvenance,
}

impl AgentGoalSpecRevision {
    /// Creates the first spec revision of a goal.
    pub fn initial(
        spec: AgentGoalSpec,
        provenance: AgentRevisionProvenance,
    ) -> AgentGoalResult<Self> {
        spec.validate()?;
        Ok(Self {
            schema_version: CURRENT_AGENT_GOAL_SPEC_SCHEMA_VERSION,
            revision: AgentRevisionNumber::INITIAL,
            spec,
            provenance,
        })
    }

    /// Accepts an updated spec, producing the next revision.
    pub fn updated(
        &self,
        spec: AgentGoalSpec,
        provenance: AgentRevisionProvenance,
    ) -> AgentGoalResult<Self> {
        spec.validate()?;
        Ok(Self {
            schema_version: CURRENT_AGENT_GOAL_SPEC_SCHEMA_VERSION,
            revision: self.revision.next(),
            spec,
            provenance,
        })
    }

    /// The revision number of this spec.
    #[must_use]
    pub const fn revision(&self) -> AgentRevisionNumber {
        self.revision
    }

    /// The spec itself.
    #[must_use]
    pub const fn spec(&self) -> &AgentGoalSpec {
        &self.spec
    }

    /// Who accepted this revision.
    #[must_use]
    pub const fn provenance(&self) -> &AgentRevisionProvenance {
        &self.provenance
    }
}

impl VersionedAgentRecord for AgentGoalSpecRevision {
    const RECORD_KIND: AgentRecordKind = AgentRecordKind::GoalSpec;

    fn schema_version(&self) -> StateSchemaVersion {
        self.schema_version
    }
}

/// What a creation command carries to institute a goal on its root task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalSpecDraft {
    /// The goal contract.
    pub spec: AgentGoalSpec,
    /// Who accepted the goal.
    pub provenance: AgentRevisionProvenance,
    /// Whether the goal starts `Active` in the creating transition. The
    /// default is true — creating the root task is the authorization to work.
    /// Opting out starts the goal `Proposed`, spending nothing until an
    /// explicit activation.
    #[serde(default = "default_activate_on_creation")]
    pub activate_on_creation: bool,
}

fn default_activate_on_creation() -> bool {
    true
}

/// Reference to the completion evaluation a criteria decision was made from
/// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// Slice 4.2 lands the evaluation machinery; this reference is its durable
/// shape from day one, so `Satisfied` is never constructible without naming
/// the evaluator, the criteria revision it assessed, and its evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalEvaluationRef {
    /// The evaluator or deciding policy. When the spec configures an
    /// evaluator, the decision door refuses a mismatch.
    pub evaluator: AgentPolicyRef,
    /// The criteria revision the evaluation assessed. A decision carrying a
    /// revision other than the one in force is refused.
    pub criteria_revision: AgentRevisionNumber,
    /// The evidence the evaluation rests on, when captured out of line as one
    /// unclassed artifact. Classed evidence lives in `evidence_items`.
    #[serde(default)]
    pub evidence: Option<ArtifactRef>,
    /// Attestation digest of the full evaluation record — the cryptographic
    /// binding between this reference and the record that produced it.
    #[serde(default)]
    pub digest: Option<AgentContentDigest>,
    /// The derived identity of the evaluation, when one produced this
    /// reference.
    #[serde(default)]
    pub evaluation_id: Option<crate::identity::AgentOperationId>,
    /// How the evaluation executed, when one produced this reference.
    #[serde(default)]
    pub method: Option<AgentGoalEvaluationMethodKind>,
    /// The classed evidence the verdict rests on; what the spec's
    /// `required_evidence` classes are checked against.
    #[serde(default)]
    pub evidence_items: Vec<AgentGoalEvidenceRef>,
}

/// A terminal goal decision
/// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)).
///
/// The terminal status is derived from the reason; a criteria decision must
/// carry the evaluation it rests on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalDecision {
    /// Why the goal ends, which determines the terminal status.
    pub reason: AgentGoalTerminalReason,
    /// The evaluation a criteria decision rests on.
    #[serde(default)]
    pub evaluation: Option<Box<AgentGoalEvaluationRef>>,
    /// Who decided, when the decision was commanded.
    #[serde(default)]
    pub provenance: Option<Box<AgentRevisionProvenance>>,
    /// The status revision the decider read; a stale value is refused.
    pub expected_status_revision: AgentRevisionNumber,
}

/// The persisted record of a goal's terminal decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalTerminalDecision {
    /// Why the goal ended.
    pub reason: AgentGoalTerminalReason,
    /// The evaluation a criteria decision rested on.
    #[serde(default)]
    pub evaluation: Option<Box<AgentGoalEvaluationRef>>,
}

/// The durable goal record the root task coordinates
/// ([specification 8.1](../../../docs/plans/rakka-agent/spec.md),
/// [6.3](../../../docs/plans/rakka-agent/spec.md)).
///
/// The status revision is monotonic and fences commanded transitions exactly
/// as the wake controller's lifecycle revision does: statuses recur, so a
/// stale resume replayed after a later park is rejected rather than silently
/// lifting it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalState {
    spec: AgentGoalSpecRevision,
    status: AgentGoalStatus,
    status_revision: AgentRevisionNumber,
    #[serde(default)]
    changed_by: Option<Box<AgentRevisionProvenance>>,
    #[serde(default)]
    wait: Option<AgentGoalWaitReason>,
    #[serde(default)]
    terminal: Option<Box<AgentGoalTerminalDecision>>,
    #[serde(default)]
    activated_at: Option<AgentTimestampMillis>,
    #[serde(default)]
    decided_at: Option<AgentTimestampMillis>,
}

impl AgentGoalState {
    /// Institutes the goal record.
    ///
    /// With `activate` the goal starts `Active` and stamps its activation;
    /// without, it starts `Proposed` and spends nothing until
    /// [`Self::activate`].
    #[must_use]
    pub fn new(spec: AgentGoalSpecRevision, activate: bool, now: AgentTimestampMillis) -> Self {
        let (status, activated_at) = if activate {
            (AgentGoalStatus::Active, Some(now))
        } else {
            (AgentGoalStatus::Proposed, None)
        };
        Self {
            spec,
            status,
            status_revision: AgentRevisionNumber::INITIAL,
            changed_by: None,
            wait: None,
            terminal: None,
            activated_at,
            decided_at: None,
        }
    }

    /// The spec revision in force.
    #[must_use]
    pub const fn spec(&self) -> &AgentGoalSpecRevision {
        &self.spec
    }

    /// The goal-contract status.
    #[must_use]
    pub const fn status(&self) -> AgentGoalStatus {
        self.status
    }

    /// The monotonic status revision commanded transitions fence on.
    #[must_use]
    pub const fn status_revision(&self) -> AgentRevisionNumber {
        self.status_revision
    }

    /// Who accepted the most recent commanded transition. `None` after a
    /// transition persisted policy decided.
    #[must_use]
    pub const fn changed_by(&self) -> Option<&AgentRevisionProvenance> {
        match &self.changed_by {
            Some(provenance) => Some(provenance),
            None => None,
        }
    }

    /// Why the goal is waiting, while it is.
    #[must_use]
    pub const fn wait(&self) -> Option<&AgentGoalWaitReason> {
        self.wait.as_ref()
    }

    /// The terminal decision, once one was made.
    #[must_use]
    pub fn terminal(&self) -> Option<&AgentGoalTerminalDecision> {
        self.terminal.as_deref()
    }

    /// When the goal was first activated.
    #[must_use]
    pub const fn activated_at(&self) -> Option<AgentTimestampMillis> {
        self.activated_at
    }

    /// When the terminal decision was made.
    #[must_use]
    pub const fn decided_at(&self) -> Option<AgentTimestampMillis> {
        self.decided_at
    }

    fn fence(&self, expected: AgentRevisionNumber) -> AgentGoalResult<()> {
        if self.status.is_terminal() {
            return Err(AgentGoalError::Terminal {
                status: self.status,
            });
        }
        if expected != self.status_revision {
            return Err(AgentGoalError::StaleStatusRevision {
                expected,
                current: self.status_revision,
            });
        }
        Ok(())
    }

    /// Activates a `Proposed` goal under an authorized command.
    pub fn activate(
        &mut self,
        expected: AgentRevisionNumber,
        provenance: AgentRevisionProvenance,
        now: AgentTimestampMillis,
    ) -> AgentGoalResult<AgentRevisionNumber> {
        self.fence(expected)?;
        if self.status != AgentGoalStatus::Proposed {
            return Err(AgentGoalError::NotProposed {
                status: self.status,
            });
        }
        self.status = AgentGoalStatus::Active;
        self.activated_at = Some(now);
        self.changed_by = Some(Box::new(provenance));
        self.status_revision = self.status_revision.next();
        Ok(self.status_revision)
    }

    /// Parks an `Active` goal `Waiting` under a persisted policy decision.
    ///
    /// Parking is entity-internal — a policy consulted inside a transition —
    /// so it takes no expected revision and clears `changed_by`: no principal
    /// decided it. Re-parking a goal already waiting on the same reason is
    /// idempotent and moves nothing; a different reason is a new fact and
    /// advances the revision.
    pub fn park(
        &mut self,
        reason: AgentGoalWaitReason,
        _now: AgentTimestampMillis,
    ) -> AgentGoalResult<AgentRevisionNumber> {
        if self.status.is_terminal() {
            return Err(AgentGoalError::Terminal {
                status: self.status,
            });
        }
        match self.status {
            AgentGoalStatus::Waiting if self.wait.as_ref() == Some(&reason) => {
                Ok(self.status_revision)
            }
            AgentGoalStatus::Waiting | AgentGoalStatus::Active => {
                self.status = AgentGoalStatus::Waiting;
                self.wait = Some(reason);
                self.changed_by = None;
                self.status_revision = self.status_revision.next();
                Ok(self.status_revision)
            }
            status => Err(AgentGoalError::NotActive { status }),
        }
    }

    /// Reactivates a `Waiting` goal under an authorized command.
    pub fn reactivate(
        &mut self,
        expected: AgentRevisionNumber,
        provenance: AgentRevisionProvenance,
        now: AgentTimestampMillis,
    ) -> AgentGoalResult<AgentRevisionNumber> {
        self.fence(expected)?;
        if self.status != AgentGoalStatus::Waiting {
            return Err(AgentGoalError::NotWaiting {
                status: self.status,
            });
        }
        self.status = AgentGoalStatus::Active;
        self.wait = None;
        if self.activated_at.is_none() {
            self.activated_at = Some(now);
        }
        self.changed_by = Some(Box::new(provenance));
        self.status_revision = self.status_revision.next();
        Ok(self.status_revision)
    }

    /// Records a terminal decision.
    ///
    /// The terminal status is [`AgentGoalDecision::reason`]'s mapping. A
    /// criteria decision — `Satisfied` or `Unsatisfied` — must carry the
    /// evaluation it rests on, and that evaluation must have assessed the
    /// criteria revision in force
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md)). From
    /// `Proposed` only `Cancelled` and `Expired` are reachable: no work
    /// happened, so no execution failure and no evaluation can exist. The
    /// reason's string payloads are truncated to
    /// [`AGENT_GOAL_REASON_MAX_LENGTH`] before they are persisted, so a
    /// caller-supplied reason cannot grow the durable record unboundedly.
    pub fn decide(
        &mut self,
        decision: AgentGoalDecision,
        now: AgentTimestampMillis,
    ) -> AgentGoalResult<AgentRevisionNumber> {
        self.fence(decision.expected_status_revision)?;
        let outcome = decision.reason.status();
        if self.status == AgentGoalStatus::Proposed
            && !matches!(
                outcome,
                AgentGoalStatus::Cancelled | AgentGoalStatus::Expired
            )
        {
            return Err(AgentGoalError::DecisionFromProposed {
                reason: decision.reason.code(),
            });
        }
        if decision.reason.requires_evaluation() {
            let Some(evaluation) = decision.evaluation.as_deref() else {
                return Err(AgentGoalError::CriteriaDecisionWithoutEvaluation {
                    reason: decision.reason.code(),
                });
            };
            let spec = self.spec.spec();
            let current = spec.criteria.revision;
            if evaluation.criteria_revision != current {
                return Err(AgentGoalError::EvaluationStale {
                    evaluated: evaluation.criteria_revision,
                    current,
                });
            }
            // A configured evaluator is the only judge the contract accepts;
            // the reference's evaluator is recorded either way, but under a
            // configured one it must match. Without one, the commander's
            // authority is the application's, and any evaluator is recorded.
            if let Some(expected) = spec.evaluator.as_ref() {
                if evaluation.evaluator != *expected {
                    return Err(AgentGoalError::EvaluatorMismatch {
                        expected: expected.clone(),
                        presented: evaluation.evaluator.clone(),
                    });
                }
            }
            if evaluation.evidence_items.len() > AGENT_GOAL_EVALUATION_MAX_EVIDENCE {
                return Err(AgentGoalError::EvaluationEvidenceInvalid {
                    length: evaluation.evidence_items.len(),
                    maximum: AGENT_GOAL_EVALUATION_MAX_EVIDENCE,
                });
            }
            for item in &evaluation.evidence_items {
                if item.class.is_empty() || item.class.len() > AGENT_GOAL_EVIDENCE_CLASS_MAX_LENGTH
                {
                    return Err(AgentGoalError::EvaluationEvidenceInvalid {
                        length: item.class.len(),
                        maximum: AGENT_GOAL_EVIDENCE_CLASS_MAX_LENGTH,
                    });
                }
            }
            // Every required class must be covered by a classed item. The
            // legacy unclassed artifact covers nothing: a class the spec
            // demands is a class the evaluation must name.
            if let Some(class) = spec.required_evidence.iter().find(|class| {
                !evaluation
                    .evidence_items
                    .iter()
                    .any(|item| item.class == **class)
            }) {
                return Err(AgentGoalError::EvidenceMissing {
                    class: class.clone(),
                });
            }
        }
        // The record stays bounded whatever the caller sent: a decision's
        // string payloads are operator text, and operator text is truncated —
        // the wake-lifecycle and task-cancellation idiom — never trusted to
        // size the durable record.
        let reason = match decision.reason {
            AgentGoalTerminalReason::ExecutionFailed { code } => {
                AgentGoalTerminalReason::ExecutionFailed {
                    code: bounded_reason_text(code),
                }
            }
            AgentGoalTerminalReason::CancellationRequested { reason } => {
                AgentGoalTerminalReason::CancellationRequested {
                    reason: bounded_reason_text(reason),
                }
            }
            other => other,
        };
        self.status = outcome;
        self.wait = None;
        self.terminal = Some(Box::new(AgentGoalTerminalDecision {
            reason,
            evaluation: decision.evaluation,
        }));
        self.changed_by = decision.provenance;
        self.decided_at = Some(now);
        self.status_revision = self.status_revision.next();
        Ok(self.status_revision)
    }

    /// Expires the goal when its own deadline has passed. Observed
    /// opportunistically at every goal entry point, which is what makes
    /// `Expired` reachable for a finite goal without timer machinery.
    pub fn observe_deadline(&mut self, now: AgentTimestampMillis) -> Option<AgentGoalStatus> {
        if self.status.is_terminal() {
            return None;
        }
        let deadline = self.spec.spec().deadline?;
        if now.as_millis() < deadline.as_millis() {
            return None;
        }
        self.status = AgentGoalStatus::Expired;
        self.wait = None;
        self.terminal = Some(Box::new(AgentGoalTerminalDecision {
            reason: AgentGoalTerminalReason::DeadlineExpired,
            evaluation: None,
        }));
        self.changed_by = None;
        self.decided_at = Some(now);
        self.status_revision = self.status_revision.next();
        Some(AgentGoalStatus::Expired)
    }

    /// Accepts a revised success criteria under an authorized command,
    /// advancing both the criteria revision and the spec revision
    /// ([specification 8.3](../../../docs/plans/rakka-agent/spec.md): if the
    /// goal changes, evaluations against the old revision are invalid).
    ///
    /// The command fences on the criteria revision it read, not the status
    /// revision — the status does not move, so a concurrent park or resume
    /// must not refuse a criteria revision, and a stale criteria read must
    /// refuse even when the status never moved. An in-flight evaluation is
    /// invalidated purely by the existing staleness fence: it completes,
    /// arrives, and is refused `goal-evaluation-stale`.
    ///
    /// Returns the criteria revision now in force.
    pub fn revise_criteria(
        &mut self,
        expected_criteria_revision: AgentRevisionNumber,
        source: AgentGoalCriteriaSource,
        digest: Option<AgentContentDigest>,
        provenance: AgentRevisionProvenance,
    ) -> AgentGoalResult<AgentRevisionNumber> {
        if self.status.is_terminal() {
            return Err(AgentGoalError::Terminal {
                status: self.status,
            });
        }
        let current = self.spec.spec().criteria.revision;
        if expected_criteria_revision != current {
            return Err(AgentGoalError::StaleCriteriaRevision {
                expected: expected_criteria_revision,
                current,
            });
        }
        let mut spec = self.spec.spec().clone();
        let revision = current.next();
        spec.criteria = AgentGoalCriteria {
            source,
            revision,
            digest,
        };
        self.spec = self.spec.updated(spec, provenance)?;
        Ok(revision)
    }
}

/// The compact goal result a task outcome carries, so a replayed command
/// answers with the goal status its transition produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalOutcome {
    /// The goal-contract status after the transition.
    pub status: AgentGoalStatus,
    /// The status revision in force after the transition.
    pub status_revision: AgentRevisionNumber,
}

/// A bounded, credential-free projection of one goal's contract status,
/// carried by the coordinating task's snapshot
/// ([specification 17.18](../../../docs/plans/rakka-agent/spec.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentGoalStatusView {
    /// The goal-contract status.
    pub status: AgentGoalStatus,
    /// The status revision commanded transitions fence on.
    pub status_revision: AgentRevisionNumber,
    /// The spec revision in force.
    pub spec_revision: AgentRevisionNumber,
    /// The criteria revision a completion evaluation must assess.
    pub criteria_revision: AgentRevisionNumber,
    /// The configured completion evaluator a criteria decision must come
    /// from, when the spec names one.
    #[serde(default)]
    pub evaluator: Option<AgentPolicyRef>,
    /// Why the goal is waiting, while it is.
    pub wait: Option<AgentGoalWaitReason>,
    /// Why the goal ended, once it did.
    pub terminal: Option<AgentGoalTerminalReason>,
    /// The run currently coordinating the goal, derived from the root task's
    /// assignment ([specification 8.1](../../../docs/plans/rakka-agent/spec.md)
    /// "current coordinator when applicable").
    pub coordinator: Option<AgentRunId>,
    /// When the goal was first activated.
    pub activated_at: Option<AgentTimestampMillis>,
    /// When the terminal decision was made.
    pub decided_at: Option<AgentTimestampMillis>,
}

/// Error raised by goal contract construction and transitions.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentGoalError {
    /// The goal already reached a terminal status.
    Terminal {
        /// The absorbing status.
        status: AgentGoalStatus,
    },
    /// A commanded transition read a status revision that has moved on.
    StaleStatusRevision {
        /// The revision the command expected.
        expected: AgentRevisionNumber,
        /// The revision in force.
        current: AgentRevisionNumber,
    },
    /// Activation requires a `Proposed` goal.
    NotProposed {
        /// The status the goal is in.
        status: AgentGoalStatus,
    },
    /// Reactivation requires a `Waiting` goal.
    NotWaiting {
        /// The status the goal is in.
        status: AgentGoalStatus,
    },
    /// Parking requires an `Active` or `Waiting` goal.
    NotActive {
        /// The status the goal is in.
        status: AgentGoalStatus,
    },
    /// A `Proposed` goal can only be cancelled or expired.
    DecisionFromProposed {
        /// The refused reason's code.
        reason: &'static str,
    },
    /// A criteria decision carried no evaluation.
    CriteriaDecisionWithoutEvaluation {
        /// The refused reason's code.
        reason: &'static str,
    },
    /// A criteria decision's evaluation assessed a revision no longer in
    /// force.
    EvaluationStale {
        /// The revision the evaluation assessed.
        evaluated: AgentRevisionNumber,
        /// The revision in force.
        current: AgentRevisionNumber,
    },
    /// A criteria decision's evaluation names an evaluator other than the
    /// configured one.
    EvaluatorMismatch {
        /// The evaluator the spec configures.
        expected: AgentPolicyRef,
        /// The evaluator the reference presented.
        presented: AgentPolicyRef,
    },
    /// A required evidence class is not covered by the evaluation's classed
    /// evidence.
    EvidenceMissing {
        /// The uncovered class.
        class: String,
    },
    /// The evaluation's classed evidence violates a bounded invariant.
    EvaluationEvidenceInvalid {
        /// The offending count or length.
        length: usize,
        /// The bound.
        maximum: usize,
    },
    /// A criteria revision command read a revision that has moved on.
    StaleCriteriaRevision {
        /// The revision the command expected.
        expected: AgentRevisionNumber,
        /// The revision in force.
        current: AgentRevisionNumber,
    },
    /// A stagnation threshold is below its minimum.
    StagnationThresholdTooLow {
        /// The offending field.
        field: &'static str,
        /// The configured value.
        value: u32,
        /// The minimum a set threshold may take.
        minimum: u32,
    },
    /// The stagnation policy selects `Replan`, which no slice can execute yet.
    StagnationReplanUnsupported,
    /// The declared fan-in quorum can never resolve.
    FanInQuorumInvalid {
        /// The declared quorum.
        n: u32,
        /// The structural membership bound.
        maximum: u32,
    },
    /// The objective summary exceeds its bound.
    SummaryTooLong {
        /// Actual length in bytes.
        length: usize,
        /// The bound.
        maximum: usize,
    },
    /// An allowed-reference set exceeds its bound.
    CollectionTooLarge {
        /// The offending field.
        field: &'static str,
        /// Actual entry count.
        length: usize,
        /// The bound.
        maximum: usize,
    },
    /// A bounded label exceeds its bound.
    LabelTooLong {
        /// The offending field.
        field: &'static str,
        /// Actual length in bytes.
        length: usize,
        /// The bound.
        maximum: usize,
    },
    /// The serialized spec exceeds its bound.
    SpecTooLarge {
        /// Actual serialized size in bytes.
        bytes: usize,
        /// The bound.
        maximum: usize,
    },
}

impl AgentGoalError {
    /// Stable machine-readable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Terminal { .. } => "goal-terminal",
            Self::StaleStatusRevision { .. } => "goal-stale-status-revision",
            Self::NotProposed { .. } => "goal-not-proposed",
            Self::NotWaiting { .. } => "goal-not-waiting",
            Self::NotActive { .. } => "goal-not-active",
            Self::DecisionFromProposed { .. } => "goal-decision-from-proposed",
            Self::CriteriaDecisionWithoutEvaluation { .. } => "goal-decision-without-evaluation",
            Self::EvaluationStale { .. } => "goal-evaluation-stale",
            Self::EvaluatorMismatch { .. } => "goal-evaluator-mismatch",
            Self::EvidenceMissing { .. } => "goal-evidence-missing",
            Self::EvaluationEvidenceInvalid { .. } => "goal-evaluation-evidence-invalid",
            Self::StaleCriteriaRevision { .. } => "goal-stale-criteria-revision",
            Self::StagnationThresholdTooLow { .. } => "goal-stagnation-threshold-too-low",
            Self::StagnationReplanUnsupported => "goal-stagnation-replan-unsupported",
            Self::FanInQuorumInvalid { .. } => "fan-in-quorum-invalid",
            Self::SummaryTooLong { .. } => "goal-summary-too-long",
            Self::CollectionTooLarge { .. } => "goal-collection-too-large",
            Self::LabelTooLong { .. } => "goal-label-too-long",
            Self::SpecTooLarge { .. } => "goal-spec-too-large",
        }
    }
}

impl Display for AgentGoalError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Terminal { status } => {
                write!(f, "the goal is terminal in status {status}")
            }
            Self::StaleStatusRevision { expected, current } => write!(
                f,
                "the command expected status revision {expected} but {current} is in force"
            ),
            Self::NotProposed { status } => {
                write!(f, "activation requires a proposed goal, not {status}")
            }
            Self::NotWaiting { status } => {
                write!(f, "reactivation requires a waiting goal, not {status}")
            }
            Self::NotActive { status } => {
                write!(f, "parking requires an active goal, not {status}")
            }
            Self::DecisionFromProposed { reason } => write!(
                f,
                "a proposed goal cannot end for reason {reason}: only cancellation or expiry"
            ),
            Self::CriteriaDecisionWithoutEvaluation { reason } => write!(
                f,
                "the criteria decision {reason} carried no evaluation reference"
            ),
            Self::EvaluationStale { evaluated, current } => write!(
                f,
                "the evaluation assessed criteria revision {evaluated} but {current} is in force"
            ),
            Self::EvaluatorMismatch {
                expected,
                presented,
            } => write!(
                f,
                "the evaluation names evaluator {presented} but the spec configures {expected}"
            ),
            Self::EvidenceMissing { class } => write!(
                f,
                "the required evidence class {class} is not covered by the evaluation"
            ),
            Self::EvaluationEvidenceInvalid { length, maximum } => write!(
                f,
                "the evaluation's classed evidence violates a bound: {length} against {maximum}"
            ),
            Self::StaleCriteriaRevision { expected, current } => write!(
                f,
                "the command expected criteria revision {expected} but {current} is in force"
            ),
            Self::StagnationThresholdTooLow {
                field,
                value,
                minimum,
            } => write!(
                f,
                "the stagnation threshold {field} is {value}, below its minimum of {minimum}"
            ),
            Self::StagnationReplanUnsupported => write!(
                f,
                "the stagnation policy selects replan, which no slice can execute yet"
            ),
            Self::FanInQuorumInvalid { n, maximum } => write!(
                f,
                "a fan-in quorum of {n} can never resolve: the quorum must be between 1 and the \
                 {maximum}-member structural bound"
            ),
            Self::SummaryTooLong { length, maximum } => write!(
                f,
                "the objective summary is {length} bytes, which exceeds the {maximum} byte bound"
            ),
            Self::CollectionTooLarge {
                field,
                length,
                maximum,
            } => write!(
                f,
                "{field} has {length} entries, which exceeds the {maximum} entry bound"
            ),
            Self::LabelTooLong {
                field,
                length,
                maximum,
            } => write!(
                f,
                "a {field} label is {length} bytes, which exceeds the {maximum} byte bound"
            ),
            Self::SpecTooLarge { bytes, maximum } => write!(
                f,
                "the goal spec serializes to {bytes} bytes, which exceeds the {maximum} byte bound"
            ),
        }
    }
}

impl Error for AgentGoalError {}

#[cfg(test)]
mod tests {
    use super::*;
    use rakka_agent_workflow::{AgentAuditEventId, AgentCausationId};

    const NOW: AgentTimestampMillis = AgentTimestampMillis::new(1_000);
    const LATER: AgentTimestampMillis = AgentTimestampMillis::new(2_000);

    fn provenance(at: u64) -> AgentRevisionProvenance {
        AgentRevisionProvenance {
            principal: PrincipalRef {
                principal_type: "service".to_string(),
                principal_id: "operator".to_string(),
                display_name: None,
            },
            accepted_at: AgentTimestampMillis::new(at),
            causation_id: AgentCausationId::new(format!("cause-{at}")),
            audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
        }
    }

    fn spec() -> AgentGoalSpec {
        AgentGoalSpec {
            owner: PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "owner".to_string(),
                display_name: None,
            },
            objective: AgentGoalObjective {
                artifact: None,
                summary: "reconcile the nightly ledger".to_string(),
            },
            criteria: AgentGoalCriteria {
                source: AgentGoalCriteriaSource::Policy(
                    AgentPolicyRef::new("ledger-balanced").expect("policy ref should be valid"),
                ),
                revision: AgentRevisionNumber::INITIAL,
                digest: None,
            },
            priority: None,
            deadline: None,
            cancellation: None,
            allocation: AgentBudgetAllocation::unbounded(),
            limits: AgentBudgetLimits::unbounded(),
            delegation: None,
            fan_in: None,
            exhaustion: AgentGoalExhaustionPolicy::default(),
            allowed_skills: BTreeSet::new(),
            allowed_tools: BTreeSet::new(),
            allowed_workflows: BTreeSet::new(),
            knowledge_spaces: BTreeSet::new(),
            environments: BTreeSet::new(),
            evaluator: None,
            required_evidence: BTreeSet::new(),
            escalation: None,
            terminal_decision: None,
            stagnation: None,
            stagnation_policy: AgentGoalStagnationPolicy::default(),
            settings_revision: None,
            policy_revision: None,
        }
    }

    fn revision() -> AgentGoalSpecRevision {
        AgentGoalSpecRevision::initial(spec(), provenance(500)).expect("spec should validate")
    }

    fn active_state() -> AgentGoalState {
        AgentGoalState::new(revision(), true, NOW)
    }

    fn proposed_state() -> AgentGoalState {
        AgentGoalState::new(revision(), false, NOW)
    }

    fn evaluation() -> AgentGoalEvaluationRef {
        AgentGoalEvaluationRef {
            evaluator: AgentPolicyRef::new("ledger-evaluator").expect("ref should be valid"),
            criteria_revision: AgentRevisionNumber::INITIAL,
            evidence: None,
            digest: None,
            evaluation_id: None,
            method: None,
            evidence_items: Vec::new(),
        }
    }

    fn decision(
        reason: AgentGoalTerminalReason,
        evaluation: Option<AgentGoalEvaluationRef>,
        expected: AgentRevisionNumber,
    ) -> AgentGoalDecision {
        AgentGoalDecision {
            reason,
            evaluation: evaluation.map(Box::new),
            provenance: Some(Box::new(provenance(900))),
            expected_status_revision: expected,
        }
    }

    fn exhaustion() -> AgentBudgetExhaustion {
        AgentBudgetExhaustion::new(AgentBudgetDimension::ModelCalls, 10, 10)
    }

    const TERMINAL_REASONS: [fn() -> AgentGoalTerminalReason; 6] = [
        || AgentGoalTerminalReason::CriteriaSatisfied,
        || AgentGoalTerminalReason::CriteriaNotMet,
        || AgentGoalTerminalReason::ExecutionFailed {
            code: "loop-failed".to_string(),
        },
        || AgentGoalTerminalReason::CancellationRequested {
            reason: "operator".to_string(),
        },
        || AgentGoalTerminalReason::DeadlineExpired,
        || AgentGoalTerminalReason::Stagnant {
            trigger: AgentStagnationTrigger::RepeatedResult,
            epochs: 3,
        },
    ];

    #[test]
    fn instituted_goal_starts_active_or_proposed() {
        let active = active_state();
        assert_eq!(active.status(), AgentGoalStatus::Active);
        assert_eq!(active.activated_at(), Some(NOW));

        let proposed = proposed_state();
        assert_eq!(proposed.status(), AgentGoalStatus::Proposed);
        assert_eq!(proposed.activated_at(), None);
        assert!(!proposed.status().permits_work());
    }

    #[test]
    fn activation_requires_proposed_and_advances_the_revision() {
        let mut state = proposed_state();
        let advanced = state
            .activate(AgentRevisionNumber::INITIAL, provenance(600), LATER)
            .expect("activation from proposed should succeed");
        assert_eq!(advanced, AgentRevisionNumber::INITIAL.next());
        assert_eq!(state.status(), AgentGoalStatus::Active);
        assert_eq!(state.activated_at(), Some(LATER));
        assert!(state.changed_by().is_some());

        let error = state
            .activate(advanced, provenance(601), LATER)
            .expect_err("an active goal cannot be activated again");
        assert_eq!(error.code(), "goal-not-proposed");
    }

    #[test]
    fn park_and_reactivate_cycle_between_active_and_waiting() {
        let mut state = active_state();
        let reason = AgentGoalWaitReason::BudgetExhausted {
            exhaustion: exhaustion(),
        };
        let parked = state
            .park(reason.clone(), LATER)
            .expect("park should succeed");
        assert_eq!(state.status(), AgentGoalStatus::Waiting);
        assert_eq!(
            state.wait().map(AgentGoalWaitReason::code),
            Some("budget-exhausted")
        );
        assert!(state.changed_by().is_none());

        // Re-parking on the same reason moves nothing.
        let reparked = state
            .park(reason, LATER)
            .expect("re-park should be idempotent");
        assert_eq!(reparked, parked);

        // A different reason is a new fact.
        let escalated = state
            .park(
                AgentGoalWaitReason::Escalated {
                    exhaustion: exhaustion(),
                },
                LATER,
            )
            .expect("a new reason should re-park");
        assert_eq!(escalated, parked.next());

        let resumed = state
            .reactivate(escalated, provenance(700), LATER)
            .expect("reactivation from waiting should succeed");
        assert_eq!(state.status(), AgentGoalStatus::Active);
        assert!(state.wait().is_none());
        assert_eq!(resumed, escalated.next());

        let error = state
            .reactivate(resumed, provenance(701), LATER)
            .expect_err("an active goal cannot be reactivated");
        assert_eq!(error.code(), "goal-not-waiting");
    }

    #[test]
    fn parking_a_proposed_goal_is_refused() {
        let mut state = proposed_state();
        let error = state
            .park(
                AgentGoalWaitReason::BudgetExhausted {
                    exhaustion: exhaustion(),
                },
                LATER,
            )
            .expect_err("a proposed goal spends nothing, so nothing can park it");
        assert_eq!(error.code(), "goal-not-active");
    }

    #[test]
    fn every_terminal_reason_is_reachable_from_active_and_waiting() {
        for build in TERMINAL_REASONS {
            for waiting in [false, true] {
                let mut state = active_state();
                let mut expected = AgentRevisionNumber::INITIAL;
                if waiting {
                    expected = state
                        .park(
                            AgentGoalWaitReason::BudgetExhausted {
                                exhaustion: exhaustion(),
                            },
                            LATER,
                        )
                        .expect("park should succeed");
                }
                let reason = build();
                let evaluation = reason.requires_evaluation().then(evaluation);
                let outcome = reason.status();
                state
                    .decide(decision(reason, evaluation, expected), LATER)
                    .expect("the terminal decision should be accepted");
                assert_eq!(state.status(), outcome);
                assert!(state.status().is_terminal());
                assert!(state.wait().is_none());
                assert_eq!(state.decided_at(), Some(LATER));
                assert!(state.terminal().is_some());
            }
        }
    }

    #[test]
    fn terminal_statuses_are_absorbing() {
        let mut state = active_state();
        let revision = state
            .decide(
                decision(
                    AgentGoalTerminalReason::CancellationRequested {
                        reason: "operator".to_string(),
                    },
                    None,
                    AgentRevisionNumber::INITIAL,
                ),
                LATER,
            )
            .expect("cancellation should be accepted");

        let error = state
            .decide(
                decision(AgentGoalTerminalReason::RootTaskCancelled, None, revision),
                LATER,
            )
            .expect_err("a terminal goal refuses further decisions");
        assert_eq!(error.code(), "goal-terminal");

        let error = state
            .reactivate(revision, provenance(800), LATER)
            .expect_err("a terminal goal cannot be reactivated");
        assert_eq!(error.code(), "goal-terminal");

        let error = state
            .park(
                AgentGoalWaitReason::BudgetExhausted {
                    exhaustion: exhaustion(),
                },
                LATER,
            )
            .expect_err("a terminal goal cannot be parked");
        assert_eq!(error.code(), "goal-terminal");
    }

    #[test]
    fn a_proposed_goal_only_cancels_or_expires() {
        for build in TERMINAL_REASONS {
            let reason = build();
            let mut state = proposed_state();
            let evaluation = reason.requires_evaluation().then(evaluation);
            let outcome = reason.status();
            let result = state.decide(
                decision(reason, evaluation, AgentRevisionNumber::INITIAL),
                LATER,
            );
            if matches!(
                outcome,
                AgentGoalStatus::Cancelled | AgentGoalStatus::Expired
            ) {
                result.expect("cancellation and expiry are reachable from proposed");
                assert_eq!(state.status(), outcome);
            } else {
                let error = result.expect_err("nothing ran, so no other outcome can exist");
                assert_eq!(error.code(), "goal-decision-from-proposed");
                assert_eq!(state.status(), AgentGoalStatus::Proposed);
            }
        }
    }

    #[test]
    fn criteria_decisions_require_an_evaluation_of_the_current_revision() {
        for reason in [
            AgentGoalTerminalReason::CriteriaSatisfied,
            AgentGoalTerminalReason::CriteriaNotMet,
        ] {
            let mut state = active_state();
            let error = state
                .decide(
                    decision(reason.clone(), None, AgentRevisionNumber::INITIAL),
                    LATER,
                )
                .expect_err("a criteria decision without an evaluation is refused");
            assert_eq!(error.code(), "goal-decision-without-evaluation");

            let mut stale = evaluation();
            stale.criteria_revision = AgentRevisionNumber::new(7);
            let error = state
                .decide(
                    decision(reason.clone(), Some(stale), AgentRevisionNumber::INITIAL),
                    LATER,
                )
                .expect_err("an evaluation of another criteria revision is refused");
            assert_eq!(error.code(), "goal-evaluation-stale");
            assert_eq!(state.status(), AgentGoalStatus::Active);

            state
                .decide(
                    decision(reason, Some(evaluation()), AgentRevisionNumber::INITIAL),
                    LATER,
                )
                .expect("an evaluation of the current revision is accepted");
            assert!(state.status().is_terminal());
        }
    }

    #[test]
    fn stale_status_revisions_are_fenced() {
        let mut state = active_state();
        let stale = AgentRevisionNumber::new(9);
        let error = state
            .decide(
                decision(AgentGoalTerminalReason::RootTaskCancelled, None, stale),
                LATER,
            )
            .expect_err("a stale expected revision is refused");
        assert_eq!(error.code(), "goal-stale-status-revision");

        let error = state
            .activate(stale, provenance(650), LATER)
            .expect_err("activation under a stale revision is refused");
        assert_eq!(error.code(), "goal-stale-status-revision");
    }

    #[test]
    fn a_passed_deadline_expires_the_goal() {
        let mut deadline_spec = spec();
        deadline_spec.deadline = Some(AgentTimestampMillis::new(1_500));
        let revision = AgentGoalSpecRevision::initial(deadline_spec, provenance(500))
            .expect("spec should validate");
        let mut state = AgentGoalState::new(revision, true, NOW);

        assert_eq!(state.observe_deadline(NOW), None);
        assert_eq!(
            state.observe_deadline(LATER),
            Some(AgentGoalStatus::Expired)
        );
        assert_eq!(state.status(), AgentGoalStatus::Expired);
        assert_eq!(
            state.terminal().map(|decision| decision.reason.code()),
            Some("deadline-expired")
        );
        // Absorbing: a second observation moves nothing.
        assert_eq!(state.observe_deadline(LATER), None);
    }

    #[test]
    fn exhaustion_policy_resolves_per_dimension_overrides() {
        let mut policy = AgentGoalExhaustionPolicy::default();
        assert_eq!(
            policy.action_for(AgentBudgetDimension::Tokens),
            AgentGoalExhaustionAction::Park
        );
        policy.default = AgentGoalExhaustionAction::Escalate;
        policy.overrides.insert(
            AgentBudgetDimension::Cost,
            AgentGoalExhaustionAction::Terminate,
        );
        assert_eq!(
            policy.action_for(AgentBudgetDimension::Cost),
            AgentGoalExhaustionAction::Terminate
        );
        assert_eq!(
            policy.action_for(AgentBudgetDimension::Tokens),
            AgentGoalExhaustionAction::Escalate
        );
    }

    #[test]
    fn terminal_reasons_map_to_their_statuses() {
        assert_eq!(
            AgentGoalTerminalReason::CriteriaSatisfied.status(),
            AgentGoalStatus::Satisfied
        );
        assert_eq!(
            AgentGoalTerminalReason::CriteriaNotMet.status(),
            AgentGoalStatus::Unsatisfied
        );
        assert_eq!(
            AgentGoalTerminalReason::BudgetExhausted {
                exhaustion: exhaustion()
            }
            .status(),
            AgentGoalStatus::Failed
        );
        assert_eq!(
            AgentGoalTerminalReason::Retired.status(),
            AgentGoalStatus::Cancelled
        );
        assert_eq!(
            AgentGoalTerminalReason::ScheduleExpired.status(),
            AgentGoalStatus::Expired
        );
    }

    #[test]
    fn spec_bounds_fail_closed_at_construction_and_deserialization() {
        let mut oversized = spec();
        oversized.objective.summary = "s".repeat(AGENT_GOAL_SUMMARY_MAX_LENGTH + 1);
        let error = AgentGoalSpecRevision::initial(oversized.clone(), provenance(500))
            .expect_err("an oversized summary is refused");
        assert_eq!(error.code(), "goal-summary-too-long");

        // The same violation fails closed on load: serialize the field
        // directly, bypassing construction.
        let mut valid = serde_json::to_value(spec()).expect("spec should serialize");
        valid["objective"]["summary"] =
            serde_json::Value::String("s".repeat(AGENT_GOAL_SUMMARY_MAX_LENGTH + 1));
        let error = serde_json::from_value::<AgentGoalSpec>(valid)
            .expect_err("deserialization must reject what construction rejects");
        assert!(
            error.to_string().contains("goal-summary-too-long")
                || error.to_string().contains("exceeds"),
            "unexpected error: {error}"
        );

        let mut crowded = spec();
        for index in 0..=AGENT_GOAL_MAX_ALLOWED_REFS {
            crowded.required_evidence.insert(format!("class-{index}"));
        }
        let error = crowded.validate().expect_err("an oversized set is refused");
        assert_eq!(error.code(), "goal-collection-too-large");
    }

    #[test]
    fn decision_reason_strings_are_truncated_to_their_bound() {
        // A decision's string payloads are operator text: persisted truncated,
        // never trusted to size the durable record.
        let mut state = active_state();
        state
            .decide(
                decision(
                    AgentGoalTerminalReason::CancellationRequested {
                        reason: "r".repeat(AGENT_GOAL_REASON_MAX_LENGTH * 4),
                    },
                    None,
                    AgentRevisionNumber::INITIAL,
                ),
                LATER,
            )
            .expect("the decision applies");
        let Some(AgentGoalTerminalReason::CancellationRequested { reason }) =
            state.terminal().map(|decision| decision.reason.clone())
        else {
            panic!("expected the cancellation reason on record");
        };
        assert_eq!(reason.len(), AGENT_GOAL_REASON_MAX_LENGTH);

        let mut state = active_state();
        state
            .decide(
                decision(
                    AgentGoalTerminalReason::ExecutionFailed {
                        code: "c".repeat(AGENT_GOAL_REASON_MAX_LENGTH + 100),
                    },
                    None,
                    AgentRevisionNumber::INITIAL,
                ),
                LATER,
            )
            .expect("the decision applies");
        let Some(AgentGoalTerminalReason::ExecutionFailed { code }) =
            state.terminal().map(|decision| decision.reason.clone())
        else {
            panic!("expected the failure code on record");
        };
        assert_eq!(code.len(), AGENT_GOAL_REASON_MAX_LENGTH);
    }

    #[test]
    fn pre_slice_4_2_records_still_decode_to_the_disabled_defaults() {
        // An evaluation reference persisted before the additive fields — the
        // exact old shape, simulated by stripping the new keys from the
        // current encoding.
        let mut old = serde_json::to_value(evaluation()).expect("the ref encodes");
        let map = old.as_object_mut().expect("the ref is an object");
        map.remove("evaluation_id");
        map.remove("method");
        map.remove("evidence_items");
        let decoded: AgentGoalEvaluationRef =
            serde_json::from_value(old).expect("the old shape decodes");
        assert!(decoded.evaluation_id.is_none());
        assert!(decoded.method.is_none());
        assert!(decoded.evidence_items.is_empty());

        // A goal spec persisted before the stagnation policy loads with
        // detection disabled.
        let mut old = serde_json::to_value(spec()).expect("the spec encodes");
        let map = old.as_object_mut().expect("the spec is an object");
        map.remove("stagnation_policy");
        let decoded: AgentGoalSpec = serde_json::from_value(old).expect("the old shape decodes");
        assert!(!decoded.stagnation_policy.is_enabled());
    }

    #[test]
    fn stagnation_validation_fails_closed_at_construction() {
        let mut low = spec();
        low.stagnation_policy.repeated_result_epochs = Some(1);
        assert_eq!(
            low.validate()
                .expect_err("a repeat count of one refuses")
                .code(),
            "goal-stagnation-threshold-too-low"
        );

        let mut replan = spec();
        replan.stagnation_policy.no_progress_epochs = Some(2);
        replan.stagnation_policy.overrides.insert(
            AgentStagnationTrigger::NoProgress,
            AgentGoalStagnationAction::Replan,
        );
        assert_eq!(
            replan
                .validate()
                .expect_err("a replan selection refuses")
                .code(),
            "goal-stagnation-replan-unsupported"
        );

        let mut unsatisfiable = spec();
        for index in 0..(AGENT_GOAL_EVALUATION_MAX_EVIDENCE + 1) {
            unsatisfiable
                .required_evidence
                .insert(format!("class-{index:02}"));
        }
        assert_eq!(
            unsatisfiable
                .validate()
                .expect_err("an unsatisfiable evidence demand refuses")
                .code(),
            "goal-collection-too-large"
        );
    }

    #[test]
    fn spec_revisions_advance_and_version() {
        let initial = revision();
        assert_eq!(initial.revision(), AgentRevisionNumber::INITIAL);
        assert_eq!(
            initial.schema_version(),
            CURRENT_AGENT_GOAL_SPEC_SCHEMA_VERSION
        );

        let mut updated_spec = spec();
        updated_spec.priority = Some(3);
        let updated = initial
            .updated(updated_spec, provenance(600))
            .expect("update should validate");
        assert_eq!(updated.revision(), AgentRevisionNumber::INITIAL.next());
        assert_eq!(updated.spec().priority, Some(3));
    }
}
