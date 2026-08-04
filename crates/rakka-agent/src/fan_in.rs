//! Durable fan-out groups and deterministic fan-in
//! ([specification 8.7](../../../docs/plans/rakka-agent/spec.md)).
//!
//! A run's committed delegation cells *are* its fan-out membership; this
//! module adds the one durable group beside them: the [`AgentFanInCell`],
//! opened in the same compare-and-set as the first delegation with a policy
//! taken from trusted state — the goal's envelope or the wiring's default,
//! never model output — which is what "the fan-in rule MUST be fixed in
//! durable state before results are accepted" means by construction: no
//! child can exist, let alone report, before a policy is durable.
//!
//! The model chooses *when* to wait, through the declared await verb the
//! loop intercepts; the group then closes, the run parks without a resident
//! task, and each child result is an inter-entity exchange that re-activates
//! the owner. [`evaluate_fan_in`] is a pure function of the durable cells,
//! the policy, and the parent-side timeout marks, so arrival order can never
//! change what a given durable state resolves to — recovery on any node
//! recomputes the same answer or finds it already persisted.
//!
//! Resolution decides when the wait ends, never whether the goal is
//! satisfied: the bounded result table becomes the awaiting call's tool
//! result, evidence the parent model consumes, and the task's decision door
//! and the goal's evaluator still judge
//! ([specification 8.3 and 14.4](../../../docs/plans/rakka-agent/spec.md)).

use std::collections::{BTreeMap, BTreeSet};

use rakka_agent_workflow::AgentTimestampMillis;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::delegation::{
    AgentDelegationCell, AgentDelegationError, AgentDelegationResult, AgentDelegationStatus,
    AGENT_RUN_MAX_DELEGATIONS,
};
use crate::identity::AgentDelegationId;
use crate::model::AgentToolCallId;
use crate::task::AgentTaskStatus;

/// Maximum members one fan-in group holds: the delegation cell bound, since
/// members are cell keys.
pub const AGENT_RUN_MAX_FAN_IN_MEMBERS: usize = AGENT_RUN_MAX_DELEGATIONS;

/// When a fan-in group's wait ends
/// ([specification 8.7](../../../docs/plans/rakka-agent/spec.md): all, an
/// early satisfying result, or a quorum).
///
/// Fixed from trusted state at group open and immutable thereafter. The
/// policy-evaluator variant of the specification's menu is a deferred slot
/// behind the non-exhaustive enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentFanInPolicy {
    /// Wait for every member to settle; satisfied only when all succeeded.
    All,
    /// Resolve on the first succeeded member, or unsatisfied when no member
    /// can succeed anymore.
    Any,
    /// Resolve when `n` members succeeded, or early-unsatisfied when the
    /// unresolved remainder can no longer reach `n`.
    Quorum {
        /// The quorum size; validated `1 ..= AGENT_RUN_MAX_FAN_IN_MEMBERS`.
        n: u32,
    },
}

impl AgentFanInPolicy {
    /// Rejects a quorum that can never be satisfied or never resolve early.
    pub fn validate(&self) -> AgentDelegationResult<()> {
        if let Self::Quorum { n } = self {
            let maximum = AGENT_RUN_MAX_FAN_IN_MEMBERS as u32;
            if *n < 1 || *n > maximum {
                return Err(AgentDelegationError::QuorumInvalid { n: *n, maximum });
            }
        }
        Ok(())
    }

    /// Stable kebab-case label.
    #[must_use]
    pub const fn as_label(&self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Any => "any",
            Self::Quorum { .. } => "quorum",
        }
    }
}

impl Default for AgentFanInPolicy {
    /// `All` is the safe default: it gathers every child's evidence and
    /// invents no early exit the goal did not ask for.
    fn default() -> Self {
        Self::All
    }
}

/// The closed vocabulary of the declared await verb.
///
/// Deliberately without a policy argument: the policy is trusted state fixed
/// at group open, and model output may choose when to wait, never what the
/// rule is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentFanInToolCall {
    /// An optional wait deadline, in epoch milliseconds; the envelope's
    /// deadline still bounds it.
    #[serde(default)]
    pub deadline: Option<AgentTimestampMillis>,
}

impl AgentFanInToolCall {
    /// Parses the await verb's arguments, failing closed on anything beyond
    /// the declared vocabulary.
    pub fn parse(arguments: &Value) -> AgentDelegationResult<Self> {
        if arguments.is_null() {
            return Ok(Self::default());
        }
        serde_json::from_value(arguments.clone()).map_err(|error| {
            AgentDelegationError::InvalidArguments {
                message: error.to_string(),
            }
        })
    }
}

/// How one fan-in resolution ended, as a stable bounded code.
pub mod resolution_code {
    /// Every member settled under the `All` policy.
    pub const ALL_SETTLED: &str = "all-settled";
    /// A member succeeded under the `Any` policy.
    pub const ANY_SATISFIED: &str = "any-satisfied";
    /// The quorum was reached.
    pub const QUORUM_SATISFIED: &str = "quorum-satisfied";
    /// No unresolved member could satisfy the policy anymore.
    pub const UNSATISFIABLE: &str = "unsatisfiable";
    /// The parent-side deadline marked the stragglers timed out.
    pub const TIMED_OUT: &str = "timed-out";
}

/// One fan-in group's deterministic resolution — absorbing once persisted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentFanInResolution {
    /// Whether the policy's predicate held.
    pub satisfied: bool,
    /// The members whose success satisfied it, in deterministic set order;
    /// empty when unsatisfied. Evidence, not decision: under `Any` and
    /// `Quorum` it holds the successes recorded when resolution first ran,
    /// so its contents can vary with arrival timing even though `satisfied`
    /// and `code` — the decision — cannot.
    pub satisfied_by: Vec<AgentDelegationId>,
    /// Stable resolution code ([`resolution_code`]).
    pub code: String,
    /// When the resolution was computed and persisted.
    pub resolved_at: AgentTimestampMillis,
}

/// The one durable fan-out group of a run.
///
/// Opened implicitly in the compare-and-set that commits the first
/// delegation; every later delegation joins at its own commit; the await
/// verb closes it. A resolved group is absorbing — the next delegation the
/// model commits replaces it with a fresh open group, so sequential rounds
/// of fan-out are one cell, not a history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentFanInCell {
    /// The policy fixed at group open, from trusted state.
    pub policy: AgentFanInPolicy,
    /// The member delegations; keys of the run's delegation cells.
    pub members: BTreeSet<AgentDelegationId>,
    /// The turn that opened the group.
    pub opened_turn: u64,
    /// When the group opened.
    pub opened_at: AgentTimestampMillis,
    /// Whether the await verb has closed membership.
    #[serde(default)]
    pub closed: bool,
    /// The await call the resolution answers, once closed.
    #[serde(default)]
    pub await_call: Option<AgentToolCallId>,
    /// The turn that closed the group, once closed.
    #[serde(default)]
    pub await_turn: Option<u64>,
    /// The parent-side wait deadline, when one is in force.
    #[serde(default)]
    pub deadline: Option<AgentTimestampMillis>,
    /// Members the parent marked timed out — a parent-side disposition,
    /// never a forged child result.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub timed_out: BTreeSet<AgentDelegationId>,
    /// The deterministic resolution, once computed — absorbing.
    #[serde(default)]
    pub resolution: Option<AgentFanInResolution>,
}

impl AgentFanInCell {
    /// Opens a group with its first member.
    #[must_use]
    pub fn open(
        policy: AgentFanInPolicy,
        member: AgentDelegationId,
        turn: u64,
        now: AgentTimestampMillis,
    ) -> Self {
        Self {
            policy,
            members: BTreeSet::from([member]),
            opened_turn: turn,
            opened_at: now,
            closed: false,
            await_call: None,
            await_turn: None,
            deadline: None,
            timed_out: BTreeSet::new(),
            resolution: None,
        }
    }

    /// Whether the group awaits children: closed and unresolved.
    #[must_use]
    pub const fn awaiting(&self) -> bool {
        self.closed && self.resolution.is_none()
    }
}

/// One member's disposition, read from its durable delegation cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemberDisposition {
    /// Nothing decidable yet: the send is pending, or the child exists and
    /// has not reported. An indeterminate child rests here until it
    /// reconciles child-side or the parent's deadline marks it timed out —
    /// the honest branch for an unknowable outcome.
    Unresolved,
    /// The child completed its task.
    Succeeded,
    /// The child failed, or the send settled without creating one.
    Failed,
    /// The child was cancelled.
    Cancelled,
    /// The parent's deadline marked the member timed out.
    TimedOut,
}

impl MemberDisposition {
    /// Stable kebab-case label for the bounded result table.
    const fn as_label(self) -> &'static str {
        match self {
            Self::Unresolved => "unresolved",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed-out",
        }
    }
}

fn member_disposition(
    cell: &AgentFanInCell,
    member: &AgentDelegationId,
    delegations: &BTreeMap<AgentDelegationId, Box<AgentDelegationCell>>,
) -> MemberDisposition {
    if cell.timed_out.contains(member) {
        return MemberDisposition::TimedOut;
    }
    // A member without a cell is unreachable by construction — members are
    // cell keys — but an absent cell must never resolve a policy, so it
    // reads unresolved: deny-when-unknown.
    let Some(delegation) = delegations.get(member) else {
        return MemberDisposition::Unresolved;
    };
    match &delegation.status {
        AgentDelegationStatus::Pending => MemberDisposition::Unresolved,
        AgentDelegationStatus::Conflicted { .. } | AgentDelegationStatus::Failed { .. } => {
            MemberDisposition::Failed
        }
        AgentDelegationStatus::ChildCreated { .. } => match &delegation.result {
            None => MemberDisposition::Unresolved,
            Some(result) => match result.status {
                AgentTaskStatus::Completed => MemberDisposition::Succeeded,
                AgentTaskStatus::Cancelled => MemberDisposition::Cancelled,
                _ => MemberDisposition::Failed,
            },
        },
    }
}

/// Evaluates a closed group against the durable cells: `Some` exactly when
/// the policy resolves on this state.
///
/// Pure and order-free — every input is durable state, so the same state
/// evaluates identically on any node, after any restart, in any arrival
/// order. Failed, cancelled, and timed-out members are explicit dispositions
/// the policy branches on; an indeterminate child stays unresolved until
/// reconciled or timed out ([specification 8.7]).
///
/// [specification 8.7]: ../../../docs/plans/rakka-agent/spec.md
#[must_use]
pub fn evaluate_fan_in(
    cell: &AgentFanInCell,
    delegations: &BTreeMap<AgentDelegationId, Box<AgentDelegationCell>>,
    now: AgentTimestampMillis,
) -> Option<AgentFanInResolution> {
    if !cell.awaiting() {
        return None;
    }
    let mut succeeded = Vec::new();
    let mut unresolved = 0_usize;
    let mut timed_out = 0_usize;
    for member in &cell.members {
        match member_disposition(cell, member, delegations) {
            MemberDisposition::Succeeded => succeeded.push(member.clone()),
            MemberDisposition::Unresolved => unresolved += 1,
            MemberDisposition::TimedOut => timed_out += 1,
            MemberDisposition::Failed | MemberDisposition::Cancelled => {}
        }
    }

    let resolve = |satisfied: bool, satisfied_by: Vec<AgentDelegationId>, code: &str| {
        Some(AgentFanInResolution {
            satisfied,
            satisfied_by,
            code: code.to_string(),
            resolved_at: now,
        })
    };
    let unsatisfied_code = if timed_out > 0 {
        resolution_code::TIMED_OUT
    } else {
        resolution_code::UNSATISFIABLE
    };

    match cell.policy {
        AgentFanInPolicy::All => {
            if unresolved > 0 {
                return None;
            }
            let satisfied = succeeded.len() == cell.members.len();
            let code = if satisfied {
                resolution_code::ALL_SETTLED
            } else if timed_out > 0 {
                resolution_code::TIMED_OUT
            } else {
                resolution_code::ALL_SETTLED
            };
            resolve(satisfied, succeeded, code)
        }
        AgentFanInPolicy::Any => {
            if !succeeded.is_empty() {
                return resolve(true, succeeded, resolution_code::ANY_SATISFIED);
            }
            if unresolved > 0 {
                return None;
            }
            resolve(false, Vec::new(), unsatisfied_code)
        }
        AgentFanInPolicy::Quorum { n } => {
            let n = n as usize;
            if succeeded.len() >= n {
                return resolve(true, succeeded, resolution_code::QUORUM_SATISFIED);
            }
            if succeeded.len() + unresolved >= n {
                return None;
            }
            resolve(false, Vec::new(), unsatisfied_code)
        }
    }
}

/// The members whose disposition is still unresolved: what a parent-side
/// deadline marks timed out ([specification 8.7]: timed-out children are an
/// explicit policy branch, decided by the parent, never a forged result).
///
/// [specification 8.7]: ../../../docs/plans/rakka-agent/spec.md
#[must_use]
pub fn unresolved_members(
    cell: &AgentFanInCell,
    delegations: &BTreeMap<AgentDelegationId, Box<AgentDelegationCell>>,
) -> Vec<AgentDelegationId> {
    cell.members
        .iter()
        .filter(|member| {
            member_disposition(cell, member, delegations) == MemberDisposition::Unresolved
        })
        .cloned()
        .collect()
}

/// Maximum bytes of the terminal-reason code one result-table row repeats.
///
/// The full bounded reason lives on the cell; the table repeats only enough
/// to steer the model, which is what keeps sixteen rows of maximal
/// identifiers inside the inline content bound — the growth-reserve test
/// measures exactly this shape.
const FAN_IN_TABLE_REASON_MAX_BYTES: usize = 64;

/// The bounded result table a resolved group records as the awaiting call's
/// tool result: one row of delegation identity, disposition, terminal status,
/// and reference digest per member — evidence the parent model consumes,
/// never child content, and never a goal decision
/// ([specification 8.3 and 14.4](../../../docs/plans/rakka-agent/spec.md)).
///
/// Bounded by construction: at most [`AGENT_RUN_MAX_FAN_IN_MEMBERS`] rows of
/// digest-length identities and stable codes. The child task ids are *not*
/// repeated here — each send's receipt already recorded them as that call's
/// tool result, and the delegation id keys back to the cell that holds them.
#[must_use]
pub fn fan_in_result_table(
    cell: &AgentFanInCell,
    delegations: &BTreeMap<AgentDelegationId, Box<AgentDelegationCell>>,
) -> Value {
    let children: Vec<Value> = cell
        .members
        .iter()
        .map(|member| {
            let disposition = member_disposition(cell, member, delegations);
            let result = delegations
                .get(member)
                .and_then(|cell| cell.result.as_ref());
            let reason = result
                .and_then(|result| result.terminal_reason.as_deref())
                .map(|reason| {
                    let mut reason = reason.to_string();
                    if reason.len() > FAN_IN_TABLE_REASON_MAX_BYTES {
                        let mut cut = FAN_IN_TABLE_REASON_MAX_BYTES;
                        while !reason.is_char_boundary(cut) {
                            cut -= 1;
                        }
                        reason.truncate(cut);
                    }
                    reason
                });
            serde_json::json!({
                "delegation": member.as_str(),
                "disposition": disposition.as_label(),
                "status": result.map(|result| result.status.as_label()),
                "terminal-reason": reason,
                "result-digest": result
                    .and_then(|result| result.result_digest.as_ref())
                    .map(|digest| digest.value.clone()),
            })
        })
        .collect();
    let resolution = cell.resolution.as_ref().map(|resolution| {
        serde_json::json!({
            "satisfied": resolution.satisfied,
            "code": resolution.code,
        })
    });
    serde_json::json!({
        "policy": cell.policy.as_label(),
        "resolution": resolution,
        "children": children,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::{AgentCapabilityId, AgentTaskDefinitionId};
    use crate::delegation::{
        delegation_id_for, AgentDelegationChildResult, AgentDelegationRecord, AgentDelegationTarget,
    };
    use crate::identity::{AgentId, AgentRunId, AgentRunScope, AgentTaskId, TenantId};
    use crate::task::AgentContentDigest;
    use rakka_agent_workflow::{AgentEffectId, AgentTelemetryContext};

    fn scope() -> AgentRunScope {
        AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("coordinator").expect("agent id"),
            AgentRunId::new("run-1").expect("run id"),
        )
        .expect("run scope")
    }

    fn cell_for(slot: usize) -> (AgentDelegationId, Box<AgentDelegationCell>) {
        let delegation = delegation_id_for(&scope(), 1, slot).expect("delegation id");
        let record = AgentDelegationRecord {
            delegation: delegation.clone(),
            goal: None,
            parent_task: AgentTaskId::new("ticket-1").expect("task id"),
            parent_run: scope(),
            lineage: Vec::new(),
            ancestors: Vec::new(),
            depth: 1,
            requested_skill: AgentCapabilityId::new("translation").expect("capability"),
            resolved: AgentDelegationTarget::new(
                AgentId::new("translator").expect("agent id"),
                AgentTaskDefinitionId::new("translate-document").expect("definition id"),
            ),
            a2a_message_id: delegation.as_str().to_string(),
            deduplication_key: delegation.as_str().to_string(),
            turn: 1,
            slot,
            effect: AgentEffectId::new(format!("effect-{slot}")),
            call_id: crate::model::AgentToolCallId::new(format!("call-{slot}")).expect("call id"),
            input: crate::task::AgentTaskContent::inline(serde_json::json!({"text": "hello"}))
                .expect("bounded input"),
            result_schema: None,
            budget: None,
            granted_descendants: None,
            deadline: None,
            definition_revision: crate::definition::AgentRevisionNumber::new(1),
            settings_revision: crate::definition::AgentRevisionNumber::new(1),
            telemetry: AgentTelemetryContext::default(),
            created_at: AgentTimestampMillis::new(1),
        };
        (
            delegation,
            Box::new(AgentDelegationCell::pending(Box::new(record))),
        )
    }

    fn child_created(cell: &mut AgentDelegationCell, slot: usize) {
        cell.settle_child_created(
            AgentTaskId::new(format!("child-{slot}")).expect("task id"),
            None,
            AgentTimestampMillis::new(2),
        );
    }

    fn with_result(cell: &mut AgentDelegationCell, status: AgentTaskStatus) {
        cell.record_child_result(AgentDelegationChildResult {
            status,
            terminal_reason: None,
            result_digest: Some(AgentContentDigest::sha256_of_bytes(b"result")),
            child_run: None,
            descendants_created: 0,
            recorded_at: AgentTimestampMillis::new(3),
        });
    }

    struct Group {
        cell: AgentFanInCell,
        delegations: BTreeMap<AgentDelegationId, Box<AgentDelegationCell>>,
    }

    fn make_group(policy: AgentFanInPolicy, size: usize) -> Group {
        let mut delegations = BTreeMap::new();
        let mut cell: Option<AgentFanInCell> = None;
        for slot in 0..size {
            let (id, delegation) = cell_for(slot);
            match cell.as_mut() {
                None => {
                    cell = Some(AgentFanInCell::open(
                        policy,
                        id.clone(),
                        1,
                        AgentTimestampMillis::new(1),
                    ));
                }
                Some(cell) => {
                    cell.members.insert(id.clone());
                }
            }
            delegations.insert(id, delegation);
        }
        let mut cell = cell.expect("at least one member");
        cell.closed = true;
        cell.await_call = Some(crate::model::AgentToolCallId::new("await-1").expect("call id"));
        Group { cell, delegations }
    }

    fn member_ids(group: &Group) -> Vec<AgentDelegationId> {
        group.cell.members.iter().cloned().collect()
    }

    fn evaluate(group: &Group) -> Option<AgentFanInResolution> {
        evaluate_fan_in(
            &group.cell,
            &group.delegations,
            AgentTimestampMillis::new(9),
        )
    }

    #[test]
    fn an_open_group_never_resolves() {
        let mut group = make_group(AgentFanInPolicy::All, 1);
        group.cell.closed = false;
        assert_eq!(evaluate(&group), None);
    }

    #[test]
    fn all_waits_for_every_member_and_satisfies_only_on_all_success() {
        let mut group = make_group(AgentFanInPolicy::All, 2);
        let members = member_ids(&group);
        for (slot, member) in members.iter().enumerate() {
            let cell = group.delegations.get_mut(member).expect("cell");
            child_created(cell, slot);
        }
        with_result(
            group.delegations.get_mut(&members[0]).expect("cell"),
            AgentTaskStatus::Completed,
        );
        assert_eq!(evaluate(&group), None, "one member still unresolved");

        with_result(
            group.delegations.get_mut(&members[1]).expect("cell"),
            AgentTaskStatus::Completed,
        );
        let resolution = evaluate(&group).expect("all settled");
        assert!(resolution.satisfied);
        assert_eq!(resolution.code, resolution_code::ALL_SETTLED);
        assert_eq!(resolution.satisfied_by.len(), 2);
    }

    #[test]
    fn a_failed_member_leaves_all_unsatisfied_but_gathers_everyone() {
        let mut group = make_group(AgentFanInPolicy::All, 2);
        let members = member_ids(&group);
        for (slot, member) in members.iter().enumerate() {
            let cell = group.delegations.get_mut(member).expect("cell");
            child_created(cell, slot);
        }
        with_result(
            group.delegations.get_mut(&members[0]).expect("cell"),
            AgentTaskStatus::Failed,
        );
        assert_eq!(
            evaluate(&group),
            None,
            "a failed member does not end the gathering under All"
        );
        with_result(
            group.delegations.get_mut(&members[1]).expect("cell"),
            AgentTaskStatus::Completed,
        );
        let resolution = evaluate(&group).expect("all settled");
        assert!(!resolution.satisfied);
        assert_eq!(resolution.code, resolution_code::ALL_SETTLED);
        assert_eq!(resolution.satisfied_by.len(), 1);
    }

    #[test]
    fn any_resolves_on_the_first_success_and_early_unsatisfiable() {
        let mut group = make_group(AgentFanInPolicy::Any, 2);
        let members = member_ids(&group);
        child_created(group.delegations.get_mut(&members[0]).expect("cell"), 0);
        with_result(
            group.delegations.get_mut(&members[0]).expect("cell"),
            AgentTaskStatus::Completed,
        );
        let resolution = evaluate(&group).expect("any satisfied");
        assert!(resolution.satisfied);
        assert_eq!(resolution.code, resolution_code::ANY_SATISFIED);

        // The sibling case: every member settles without a success.
        let mut group = make_group(AgentFanInPolicy::Any, 2);
        let members = member_ids(&group);
        for member in &members {
            group
                .delegations
                .get_mut(member)
                .expect("cell")
                .settle_failed("delegation-child-conflict", AgentTimestampMillis::new(2));
        }
        let resolution = evaluate(&group).expect("unsatisfiable");
        assert!(!resolution.satisfied);
        assert_eq!(resolution.code, resolution_code::UNSATISFIABLE);
    }

    #[test]
    fn quorum_resolves_at_n_and_early_exits_when_unreachable() {
        let mut group = make_group(AgentFanInPolicy::Quorum { n: 2 }, 3);
        let members = member_ids(&group);
        for (slot, member) in members.iter().enumerate() {
            child_created(group.delegations.get_mut(member).expect("cell"), slot);
        }
        with_result(
            group.delegations.get_mut(&members[0]).expect("cell"),
            AgentTaskStatus::Completed,
        );
        assert_eq!(evaluate(&group), None, "one success of two required");
        with_result(
            group.delegations.get_mut(&members[1]).expect("cell"),
            AgentTaskStatus::Completed,
        );
        let resolution = evaluate(&group).expect("quorum satisfied");
        assert!(resolution.satisfied);
        assert_eq!(resolution.code, resolution_code::QUORUM_SATISFIED);
        assert_eq!(resolution.satisfied_by.len(), 2);

        // Two failures leave one possible success against a quorum of two.
        let mut group = make_group(AgentFanInPolicy::Quorum { n: 2 }, 3);
        let members = member_ids(&group);
        for member in members.iter().take(2) {
            group
                .delegations
                .get_mut(member)
                .expect("cell")
                .settle_failed("delegation-child-conflict", AgentTimestampMillis::new(2));
        }
        let resolution = evaluate(&group).expect("early unsatisfiable");
        assert!(!resolution.satisfied);
        assert_eq!(resolution.code, resolution_code::UNSATISFIABLE);
    }

    #[test]
    fn timed_out_members_are_a_policy_branch_not_a_wait() {
        let mut group = make_group(AgentFanInPolicy::All, 2);
        let members = member_ids(&group);
        child_created(group.delegations.get_mut(&members[0]).expect("cell"), 0);
        with_result(
            group.delegations.get_mut(&members[0]).expect("cell"),
            AgentTaskStatus::Completed,
        );
        child_created(group.delegations.get_mut(&members[1]).expect("cell"), 1);
        assert_eq!(evaluate(&group), None, "the straggler still counts");

        group.cell.timed_out.insert(members[1].clone());
        let resolution = evaluate(&group).expect("timeout resolves");
        assert!(!resolution.satisfied);
        assert_eq!(resolution.code, resolution_code::TIMED_OUT);
    }

    #[test]
    fn a_cancelled_child_is_an_explicit_disposition() {
        let mut group = make_group(AgentFanInPolicy::Any, 1);
        let members = member_ids(&group);
        child_created(group.delegations.get_mut(&members[0]).expect("cell"), 0);
        with_result(
            group.delegations.get_mut(&members[0]).expect("cell"),
            AgentTaskStatus::Cancelled,
        );
        let resolution = evaluate(&group).expect("nothing can succeed");
        assert!(!resolution.satisfied);
        assert_eq!(resolution.code, resolution_code::UNSATISFIABLE);
    }

    #[test]
    fn evaluation_is_arrival_order_free() {
        // Two orders of recording the same outcomes evaluate identically:
        // the inputs are durable state, not events.
        let outcomes = [AgentTaskStatus::Completed, AgentTaskStatus::Failed];
        let mut resolutions = Vec::new();
        for order in [[0_usize, 1], [1, 0]] {
            let mut group = make_group(AgentFanInPolicy::Quorum { n: 1 }, 2);
            let members = member_ids(&group);
            for index in order {
                let member = &members[index];
                child_created(group.delegations.get_mut(member).expect("cell"), index);
                with_result(
                    group.delegations.get_mut(member).expect("cell"),
                    outcomes[index],
                );
            }
            resolutions.push(evaluate(&group).expect("resolves"));
        }
        assert_eq!(resolutions[0].satisfied, resolutions[1].satisfied);
        assert_eq!(resolutions[0].code, resolutions[1].code);
        assert_eq!(resolutions[0].satisfied_by, resolutions[1].satisfied_by);
    }

    #[test]
    fn the_quorum_bounds_are_validated() {
        assert_eq!(
            AgentFanInPolicy::Quorum { n: 0 }
                .validate()
                .expect_err("zero quorum")
                .code(),
            "fan-in-quorum-invalid"
        );
        assert_eq!(
            AgentFanInPolicy::Quorum { n: 17 }
                .validate()
                .expect_err("oversized quorum")
                .code(),
            "fan-in-quorum-invalid"
        );
        AgentFanInPolicy::Quorum { n: 16 }
            .validate()
            .expect("the bound itself is valid");
    }

    #[test]
    fn the_await_vocabulary_fails_closed_on_unknown_fields() {
        let error = AgentFanInToolCall::parse(&serde_json::json!({"policy": "any"}))
            .expect_err("the policy is not model-selectable");
        assert_eq!(error.code(), "delegation-invalid-arguments");
        assert_eq!(
            AgentFanInToolCall::parse(&Value::Null).expect("null parses"),
            AgentFanInToolCall::default()
        );
    }
}
