//! The goal contract and its lifecycle.
//!
//! Owns the goal-mode half of the contract: [`AgentGoalMode`] distinguishes a
//! finite goal, which terminates once its criteria are evaluated, from a
//! continuous one, which executes as bounded durable epochs admitted by the
//! wake controller of [`crate::wake`] — never as an immortal polling future.
//! [`AgentContinuousGoalSpec`] carries exactly what that controller needs: the
//! current schedule revision, the versioned wake policy, and the explicit
//! health condition [specification 8.1](../../../docs/plans/rakka-agent/spec.md)
//! requires of unattended operation.
//!
//! The full `AgentGoalSpec` and `AgentGoalStatus` — owner, success criteria,
//! evaluator, escalation, and the `Unsatisfied`/`Failed` distinction — land in
//! slice 4.1. The root `AgentTaskEntity` coordinates the goal; `AgentGoalId`
//! defaults to the root `AgentTaskId` value while the two types stay distinct.
//! A goal stays addressable while fully passivated.
//!
//! Specification: sections 8.1 and 6.3, with the continuous clauses of 8.2.

use serde::{Deserialize, Serialize};

use crate::definition::AgentPolicyRef;
use crate::wake::{AgentWakePolicyRevision, ScheduleRevision};

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
/// arrive with `AgentGoalSpec` in slice 4.1 and sit *around* these fields, not
/// inside them.
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
}
