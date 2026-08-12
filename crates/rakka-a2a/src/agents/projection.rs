//! The specification 14.3 task/run state projection.
//!
//! Maps the authoritative Rakka task status — refined by the current run's
//! condition while the task is in progress — onto the public A2A
//! [`TaskState`]. The mapping follows the specification 14.3 table
//! row-for-row and keeps its three structural rules:
//!
//! - A run's `HandedOff` or `Superseded` condition never makes the public
//!   task terminal while a successor run owns it; the projection follows the
//!   authoritative task status and carries the run condition as metadata.
//! - A `Suspended` run projects `WORKING` (A2A defines no paused state) with
//!   bounded suspension metadata, so a client can distinguish administrative
//!   suspension from active work.
//! - A cancellation request alone never projects `CANCELED`. While
//!   cancellation propagates the projection stays on the authoritative
//!   nonterminal condition, and a parked indeterminate effect keeps the task
//!   on `INPUT_REQUIRED` with a stable indeterminate reason until its
//!   reconciliation decision makes the task safe to close.
//!
//! `UNSPECIFIED` is never produced: an unknown future status projects the
//! neutral nonterminal `WORKING` rather than a state the projection cannot
//! stand behind.

use std::collections::HashMap;

use a2a::TaskState;
use rakka_agent::{AgentRunStatus, AgentTaskStatus};
use serde_json::Value;

/// Task-status metadata key: the authoritative task condition label.
pub const META_AGENT_TASK_CONDITION: &str = "io.rakka.agent.task-condition";

/// Task-status metadata key: the current run's condition label, when a run
/// exists. This is the bounded assignment/suspension/handoff metadata the
/// specification 14.3 rows call for.
pub const META_AGENT_RUN_CONDITION: &str = "io.rakka.agent.run-condition";

/// Task-status metadata key: the stable reason a waiting projection waits.
/// One of `input`, `approval`, `authorization`, or `indeterminate-effect`.
pub const META_AGENT_WAIT_REASON: &str = "io.rakka.agent.wait-reason";

/// Task-status metadata key: how many result proposals the task's
/// deterministic rules have refused, emitted once the count is nonzero.
///
/// The bounded rejection echo (specification 8.12, 9.2): a typed-result
/// submission whose validation rejected answers with the ordinary task view,
/// so the view itself carries the decision — for human submitters through
/// this surface and, equally, for run proposals on agent-owned tasks.
pub const META_AGENT_REJECTIONS: &str = "io.rakka.agent.rejections";

/// Task-status metadata key: the most recent rejection decision, as one
/// bounded object `{ "reason": <stable code>, "rule": <rule id when a rule
/// refused> }`. Emitted beside [`META_AGENT_REJECTIONS`].
pub const META_AGENT_LAST_REJECTION: &str = "io.rakka.agent.last-rejection";

/// The authoritative Rakka condition one public A2A task state projects from.
///
/// `task` is the owning [`rakka_agent::AgentTaskEntity`] status — always the
/// projection's anchor. `run` is the condition of the run currently assigned
/// to the task, when one exists; it refines an `InProgress` task onto the
/// waiting rows of the specification 14.3 table and otherwise rides along as
/// bounded metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentTaskCondition {
    /// The authoritative task status.
    pub task: AgentTaskStatus,
    /// The current run's status, when the task has an assigned run.
    pub run: Option<AgentRunStatus>,
}

impl AgentTaskCondition {
    /// A condition for a task with no current run.
    #[must_use]
    pub const fn task_only(task: AgentTaskStatus) -> Self {
        Self { task, run: None }
    }

    /// A condition for a task refined by its current run.
    #[must_use]
    pub const fn with_run(task: AgentTaskStatus, run: AgentRunStatus) -> Self {
        Self {
            task,
            run: Some(run),
        }
    }
}

/// Projects one authoritative condition onto the public A2A [`TaskState`]
/// per the specification 14.3 table. Never returns [`TaskState::Unspecified`].
#[must_use]
pub fn agent_task_state(condition: AgentTaskCondition) -> TaskState {
    match condition.task {
        AgentTaskStatus::Completed => TaskState::Completed,
        AgentTaskStatus::Failed => TaskState::Failed,
        AgentTaskStatus::Cancelled => TaskState::Canceled,
        AgentTaskStatus::Created | AgentTaskStatus::Blocked | AgentTaskStatus::Assigned => {
            TaskState::Submitted
        }
        AgentTaskStatus::WaitingForInput => TaskState::InputRequired,
        // `InProgress` refines on the current run's wait condition; an unknown
        // future task status takes the same neutral nonterminal path rather
        // than guessing a terminal or waiting state.
        AgentTaskStatus::InProgress | _ => match condition.run {
            Some(AgentRunStatus::WaitingForApproval | AgentRunStatus::WaitingForReconciliation) => {
                TaskState::InputRequired
            }
            Some(AgentRunStatus::WaitingForAuthorization) => TaskState::AuthRequired,
            _ => TaskState::Working,
        },
    }
}

/// The bounded task-status metadata for one authoritative condition:
/// condition labels always, and the stable wait reason on waiting rows.
#[must_use]
pub fn agent_task_state_metadata(condition: AgentTaskCondition) -> HashMap<String, Value> {
    let mut metadata = HashMap::new();
    metadata.insert(
        META_AGENT_TASK_CONDITION.to_string(),
        Value::String(task_condition_label(condition.task).to_string()),
    );
    if let Some(run) = condition.run {
        metadata.insert(
            META_AGENT_RUN_CONDITION.to_string(),
            Value::String(run_condition_label(run).to_string()),
        );
    }
    if let Some(reason) = wait_reason(condition) {
        metadata.insert(
            META_AGENT_WAIT_REASON.to_string(),
            Value::String(reason.to_string()),
        );
    }
    metadata
}

/// The stable reason a waiting projection waits, when it waits.
fn wait_reason(condition: AgentTaskCondition) -> Option<&'static str> {
    if condition.task.is_terminal() {
        return None;
    }
    if matches!(condition.task, AgentTaskStatus::WaitingForInput) {
        return Some("input");
    }
    match condition.run {
        Some(AgentRunStatus::WaitingForApproval) => Some("approval"),
        Some(AgentRunStatus::WaitingForAuthorization) => Some("authorization"),
        Some(AgentRunStatus::WaitingForReconciliation) => Some("indeterminate-effect"),
        _ => None,
    }
}

/// Stable bounded label for a task status.
const fn task_condition_label(status: AgentTaskStatus) -> &'static str {
    match status {
        AgentTaskStatus::Created => "created",
        AgentTaskStatus::Blocked => "blocked",
        AgentTaskStatus::Assigned => "assigned",
        AgentTaskStatus::InProgress => "in-progress",
        AgentTaskStatus::WaitingForInput => "waiting-for-input",
        AgentTaskStatus::Completed => "completed",
        AgentTaskStatus::Failed => "failed",
        AgentTaskStatus::Cancelled => "cancelled",
        _ => "unknown",
    }
}

/// Stable bounded label for a run status.
const fn run_condition_label(status: AgentRunStatus) -> &'static str {
    match status {
        AgentRunStatus::Accepted => "accepted",
        AgentRunStatus::Running => "running",
        AgentRunStatus::WaitingForTimer => "waiting-for-timer",
        AgentRunStatus::WaitingForEffect => "waiting-for-effect",
        AgentRunStatus::WaitingForApproval => "waiting-for-approval",
        AgentRunStatus::WaitingForAuthorization => "waiting-for-authorization",
        AgentRunStatus::WaitingForReconciliation => "waiting-for-reconciliation",
        AgentRunStatus::Suspended => "suspended",
        AgentRunStatus::Cancelling => "cancelling",
        AgentRunStatus::Compensating => "compensating",
        AgentRunStatus::HandedOff => "handed-off",
        AgentRunStatus::Superseded => "superseded",
        AgentRunStatus::Completed => "completed",
        AgentRunStatus::Failed => "failed",
        AgentRunStatus::Cancelled => "cancelled",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_TASK_STATUSES: [AgentTaskStatus; 8] = [
        AgentTaskStatus::Created,
        AgentTaskStatus::Blocked,
        AgentTaskStatus::Assigned,
        AgentTaskStatus::InProgress,
        AgentTaskStatus::WaitingForInput,
        AgentTaskStatus::Completed,
        AgentTaskStatus::Failed,
        AgentTaskStatus::Cancelled,
    ];

    const ALL_RUN_STATUSES: [AgentRunStatus; 15] = [
        AgentRunStatus::Accepted,
        AgentRunStatus::Running,
        AgentRunStatus::WaitingForTimer,
        AgentRunStatus::WaitingForEffect,
        AgentRunStatus::WaitingForApproval,
        AgentRunStatus::WaitingForAuthorization,
        AgentRunStatus::WaitingForReconciliation,
        AgentRunStatus::Suspended,
        AgentRunStatus::Cancelling,
        AgentRunStatus::Compensating,
        AgentRunStatus::HandedOff,
        AgentRunStatus::Superseded,
        AgentRunStatus::Completed,
        AgentRunStatus::Failed,
        AgentRunStatus::Cancelled,
    ];

    #[test]
    fn specification_table_row_for_row() {
        // Row 1: task Created / Blocked / Assigned -> SUBMITTED.
        for task in [
            AgentTaskStatus::Created,
            AgentTaskStatus::Blocked,
            AgentTaskStatus::Assigned,
        ] {
            assert_eq!(
                agent_task_state(AgentTaskCondition::task_only(task)),
                TaskState::Submitted,
            );
        }
        // Row 2: task InProgress with an active, timed, effect-bound,
        // suspended, cancelling, or compensating run -> WORKING.
        for run in [
            AgentRunStatus::Accepted,
            AgentRunStatus::Running,
            AgentRunStatus::WaitingForTimer,
            AgentRunStatus::WaitingForEffect,
            AgentRunStatus::Suspended,
            AgentRunStatus::Cancelling,
            AgentRunStatus::Compensating,
        ] {
            assert_eq!(
                agent_task_state(AgentTaskCondition::with_run(
                    AgentTaskStatus::InProgress,
                    run
                )),
                TaskState::Working,
            );
        }
        // Row 3: task WaitingForInput (including a human-owned task awaiting
        // its typed result) -> INPUT_REQUIRED.
        assert_eq!(
            agent_task_state(AgentTaskCondition::task_only(
                AgentTaskStatus::WaitingForInput
            )),
            TaskState::InputRequired,
        );
        // Row 4: WaitingForApproval -> INPUT_REQUIRED.
        assert_eq!(
            agent_task_state(AgentTaskCondition::with_run(
                AgentTaskStatus::InProgress,
                AgentRunStatus::WaitingForApproval,
            )),
            TaskState::InputRequired,
        );
        // Row 5: WaitingForAuthorization -> AUTH_REQUIRED.
        assert_eq!(
            agent_task_state(AgentTaskCondition::with_run(
                AgentTaskStatus::InProgress,
                AgentRunStatus::WaitingForAuthorization,
            )),
            TaskState::AuthRequired,
        );
        // Row 6: WaitingForReconciliation -> INPUT_REQUIRED with a stable
        // indeterminate reason.
        let reconciliation = AgentTaskCondition::with_run(
            AgentTaskStatus::InProgress,
            AgentRunStatus::WaitingForReconciliation,
        );
        assert_eq!(agent_task_state(reconciliation), TaskState::InputRequired);
        assert_eq!(
            agent_task_state_metadata(reconciliation).get(META_AGENT_WAIT_REASON),
            Some(&Value::String("indeterminate-effect".to_string())),
        );
        // Rows 7-9: terminal task statuses.
        assert_eq!(
            agent_task_state(AgentTaskCondition::task_only(AgentTaskStatus::Completed)),
            TaskState::Completed,
        );
        assert_eq!(
            agent_task_state(AgentTaskCondition::task_only(AgentTaskStatus::Failed)),
            TaskState::Failed,
        );
        assert_eq!(
            agent_task_state(AgentTaskCondition::task_only(AgentTaskStatus::Cancelled)),
            TaskState::Canceled,
        );
    }

    #[test]
    fn handed_off_and_superseded_runs_never_close_the_public_task() {
        for run in [AgentRunStatus::HandedOff, AgentRunStatus::Superseded] {
            let condition = AgentTaskCondition::with_run(AgentTaskStatus::InProgress, run);
            let state = agent_task_state(condition);
            assert!(!state.is_terminal(), "{run:?} projected terminal {state:?}");
            assert_eq!(state, TaskState::Working);
            // The run condition still surfaces as bounded metadata.
            assert!(agent_task_state_metadata(condition).contains_key(META_AGENT_RUN_CONDITION));
        }
    }

    #[test]
    fn suspended_projects_working_with_suspension_metadata() {
        let condition =
            AgentTaskCondition::with_run(AgentTaskStatus::InProgress, AgentRunStatus::Suspended);
        assert_eq!(agent_task_state(condition), TaskState::Working);
        assert_eq!(
            agent_task_state_metadata(condition).get(META_AGENT_RUN_CONDITION),
            Some(&Value::String("suspended".to_string())),
        );
    }

    #[test]
    fn cancellation_in_progress_never_projects_canceled() {
        // A cancellation request alone: the run is cancelling, the task is
        // still authoritative and nonterminal.
        let cancelling =
            AgentTaskCondition::with_run(AgentTaskStatus::InProgress, AgentRunStatus::Cancelling);
        assert_eq!(agent_task_state(cancelling), TaskState::Working);
        // A parked indeterminate effect during wind-down keeps the task on
        // INPUT_REQUIRED until its reconciliation decision.
        let parked = AgentTaskCondition::with_run(
            AgentTaskStatus::InProgress,
            AgentRunStatus::WaitingForReconciliation,
        );
        assert_eq!(agent_task_state(parked), TaskState::InputRequired);
        // Only the authoritative task status closes the projection.
        assert_eq!(
            agent_task_state(AgentTaskCondition::task_only(AgentTaskStatus::Cancelled)),
            TaskState::Canceled,
        );
    }

    #[test]
    fn unspecified_is_never_produced() {
        for task in ALL_TASK_STATUSES {
            let state = agent_task_state(AgentTaskCondition::task_only(task));
            assert_ne!(state, TaskState::Unspecified, "task {task:?}");
            for run in ALL_RUN_STATUSES {
                let state = agent_task_state(AgentTaskCondition::with_run(task, run));
                assert_ne!(state, TaskState::Unspecified, "task {task:?} run {run:?}");
            }
        }
    }

    #[test]
    fn wait_reasons_are_stable_labels() {
        let cases = [
            (
                AgentTaskCondition::task_only(AgentTaskStatus::WaitingForInput),
                "input",
            ),
            (
                AgentTaskCondition::with_run(
                    AgentTaskStatus::InProgress,
                    AgentRunStatus::WaitingForApproval,
                ),
                "approval",
            ),
            (
                AgentTaskCondition::with_run(
                    AgentTaskStatus::InProgress,
                    AgentRunStatus::WaitingForAuthorization,
                ),
                "authorization",
            ),
            (
                AgentTaskCondition::with_run(
                    AgentTaskStatus::InProgress,
                    AgentRunStatus::WaitingForReconciliation,
                ),
                "indeterminate-effect",
            ),
        ];
        for (condition, expected) in cases {
            assert_eq!(
                agent_task_state_metadata(condition).get(META_AGENT_WAIT_REASON),
                Some(&Value::String(expected.to_string())),
                "{condition:?}",
            );
        }
        // A terminal task never claims to wait.
        assert!(!agent_task_state_metadata(AgentTaskCondition::task_only(
            AgentTaskStatus::Cancelled
        ))
        .contains_key(META_AGENT_WAIT_REASON));
    }
}
