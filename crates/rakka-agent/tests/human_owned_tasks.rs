//! Human-owned typed tasks
//! ([specification 8.12](../../../docs/plans/rakka-agent/spec.md), scenario
//! 41): an authenticated human or external service completes a deliberately
//! unassigned task through the same deterministic validation path a run's
//! proposal travels — deduplicated, first-writer-wins, non-committing
//! refusals — and the completion unblocks the task's registered dependents,
//! while a failed human task propagates each edge's declared dependency
//! policy through the cancellation *request* path, never a direct
//! terminalization.

mod common;

use std::sync::Arc;

use common::{agent_id, task_definition, task_scope, tenant, Fixture, TASK, TENANT};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    dependency_outcome_operation_id, dependency_registration_operation_id,
    human_result_operation_id, load_agent_task_state, AgentContentDigest,
    AgentHumanResultSubmission, AgentOperationId, AgentRevisionNumber, AgentSchemaPolicy,
    AgentTaskContent, AgentTaskCreation, AgentTaskDependencyDeclaration,
    AgentTaskDependencyOutcome, AgentTaskEntityCommand, AgentTaskEntityReply, AgentTaskError,
    AgentTaskHistoryKind, AgentTaskHistoryStore, AgentTaskId, AgentTaskOwnership, AgentTaskScope,
    AgentTaskStatus, AgentTaskSubmissionDisposition, AGENT_TASK_REJECTED_SUBMISSION_ECHO_CAPACITY,
    METRIC_AGENT_HUMAN_RESULTS,
};
use rakka_agent_workflow::{AgentCausationId, AgentTimestampMillis};
use rakka_core::InMemoryMetricsRecorder;
use serde_json::json;

const HUMAN_TASK: &str = "human-review";
const PRINCIPAL: &str = "human:alice";

fn human_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(HUMAN_TASK).expect("the task id is valid"),
    )
    .expect("the scope is valid")
}

fn creation_op(task: &str) -> AgentOperationId {
    AgentOperationId::new(
        rakka_agent::AgentOperationKind::TaskCreation,
        [TENANT, task, "1"],
    )
    .expect("the operation id derives")
}

/// Creates the human-owned task, optionally depending on other tasks.
async fn create_human_task(fx: &Fixture, dependencies: Vec<AgentTaskDependencyDeclaration>) {
    fx.apply_task_command_at(
        &human_scope(),
        AgentTaskEntityCommand::Create {
            operation_id: creation_op(HUMAN_TASK),
            creation: Box::new(AgentTaskCreation {
                definition: task_definition().with_ownership(AgentTaskOwnership::Human),
                input: AgentTaskContent::inline(json!({ "ticket": 9 }))
                    .expect("the input is inline-bounded"),
                assignee: None,
                team: None,
                goal: None,
                goal_mode: Default::default(),
                goal_spec: None,
                parent: None,
                dependencies,
                escrow: None,
                wake: None,
                delegation: None,
                telemetry: Default::default(),
            }),
        },
    )
    .await
    .expect("the human task creates");
}

/// Creates the standard agent-owned dependent at the fixture's task scope,
/// blocked on the human task.
async fn create_dependent_on_human(fx: &Fixture) {
    fx.apply_task_command(AgentTaskEntityCommand::Create {
        operation_id: creation_op(TASK),
        creation: Box::new(AgentTaskCreation {
            definition: task_definition(),
            input: AgentTaskContent::inline(json!({ "ticket": 1 }))
                .expect("the input is inline-bounded"),
            assignee: Some(agent_id()),
            team: None,
            goal: None,
            goal_mode: Default::default(),
            goal_spec: None,
            parent: None,
            dependencies: vec![AgentTaskDependencyDeclaration::new(
                human_scope().task().clone(),
            )],
            escrow: None,
            wake: None,
            delegation: None,
            telemetry: Default::default(),
        }),
    })
    .await
    .expect("the dependent creates");
}

fn submission(discriminator: &str, answer: serde_json::Value) -> AgentHumanResultSubmission {
    AgentHumanResultSubmission {
        principal: PRINCIPAL.to_string(),
        definition_id: common::task_definition_id(),
        definition_version: AgentRevisionNumber::INITIAL,
        result_schema: common::schema("ticket-result"),
        content: AgentTaskContent::inline(answer).expect("the content is inline-bounded"),
        evidence: Vec::new(),
        causation_id: AgentCausationId::new(format!("cause-{discriminator}")),
        submitted_at: AgentTimestampMillis::new(0),
    }
}

fn submit_command(discriminator: &str, answer: serde_json::Value) -> AgentTaskEntityCommand {
    AgentTaskEntityCommand::SubmitHumanResult {
        operation_id: human_result_operation_id(&tenant(), human_scope().task(), discriminator)
            .expect("the operation id derives"),
        submission: Box::new(submission(discriminator, answer)),
    }
}

/// Settles both tasks for a few rounds without driving the model loop.
async fn settle_pair(fx: &Fixture) {
    for _round in 0..6 {
        let human = fx
            .settle_task_at(&human_scope())
            .await
            .expect("the human task settles");
        let dependent = fx
            .settle_task_at(&task_scope())
            .await
            .expect("the dependent settles");
        if human.outstanding == 0 && dependent.outstanding == 0 {
            return;
        }
    }
}

async fn human_state(fx: &Fixture) -> rakka_agent::AgentTaskState {
    load_agent_task_state(&fx.tasks, &human_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the task exists")
}

fn applied_submission(reply: &AgentTaskEntityReply) -> &rakka_agent::AgentTaskSubmissionDecision {
    match reply {
        AgentTaskEntityReply::Applied { outcome } | AgentTaskEntityReply::Duplicate { outcome } => {
            outcome
                .submission
                .as_deref()
                .expect("the outcome carries the submission decision")
        }
        other => panic!("the reply carries no outcome: {other:?}"),
    }
}

/// The derived operation ids are wire and durable surface: their exact
/// values are pinned so a change is a deliberate migration, never drift.
#[test]
fn the_derived_operation_ids_are_pinned() {
    let tenant = tenant();
    let human = AgentTaskId::new(HUMAN_TASK).expect("the id is valid");
    let dependent = AgentTaskId::new(TASK).expect("the id is valid");
    assert_eq!(
        human_result_operation_id(&tenant, &human, "submit-1")
            .expect("the id derives")
            .as_str(),
        "result-submission/acme/human-review/submit-1"
    );
    assert_eq!(
        dependency_registration_operation_id(&tenant, &human, &dependent)
            .expect("the id derives")
            .as_str(),
        "dependency-registration/acme/human-review/ticket-1"
    );
    assert_eq!(
        dependency_outcome_operation_id(&tenant, &human, &dependent)
            .expect("the id derives")
            .as_str(),
        "dependency-outcome/acme/human-review/ticket-1"
    );
}

/// Scenario 41's happy half: an authenticated, deduplicated completion
/// unblocks a real dependent.
#[tokio::test]
async fn an_authenticated_completion_unblocks_the_dependent() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
    .with_metrics(metrics.clone());
    fx.instantiate_agent().await;
    create_human_task(&fx, Vec::new()).await;
    create_dependent_on_human(&fx).await;

    // The dependent is born blocked; its registration reaches the upstream
    // on the settle pass.
    assert_eq!(fx.task_snapshot().await.status, AgentTaskStatus::Blocked);
    settle_pair(&fx).await;
    let human = human_state(&fx).await;
    let human_task = human.task().expect("the human task exists");
    assert_eq!(human_task.status, AgentTaskStatus::WaitingForInput);
    assert!(
        human_task.dependents.contains_key(task_scope().task()),
        "the upstream durably registered its dependent"
    );

    // The authenticated submission completes the task through the same
    // validation path a run's proposal travels.
    let reply = fx
        .apply_task_command_at(
            &human_scope(),
            submit_command("submit-1", json!({ "answer": "reviewed" })),
        )
        .await
        .expect("the submission applies");
    let decision = applied_submission(&reply);
    assert_eq!(
        decision.disposition,
        AgentTaskSubmissionDisposition::Accepted
    );

    let human = human_state(&fx).await;
    let human_task = human.task().expect("the human task exists");
    assert_eq!(human_task.status, AgentTaskStatus::Completed);
    let accepted = human_task
        .accepted_result
        .as_deref()
        .expect("the result is accepted");
    assert_eq!(accepted.principal.as_deref(), Some(PRINCIPAL));
    assert_eq!(accepted.run, None);

    // A duplicate inside the operation window answers the original outcome
    // without a second transition.
    let replay = fx
        .apply_task_command_at(
            &human_scope(),
            submit_command("submit-1", json!({ "answer": "reviewed" })),
        )
        .await
        .expect("the replay answers");
    assert!(matches!(replay, AgentTaskEntityReply::Duplicate { .. }));

    // The completion's outcome notification unblocks the dependent, whose
    // own assignment decision then proceeds.
    settle_pair(&fx).await;
    let dependent = fx.task_snapshot().await;
    assert_ne!(dependent.status, AgentTaskStatus::Blocked);
    assert!(dependent.dependencies_satisfied);

    let human = human_state(&fx).await;
    let record = human
        .task()
        .expect("the human task exists")
        .dependents
        .get(task_scope().task())
        .expect("the registry entry stands")
        .clone();
    assert!(record.outcome_settled, "the notification settled durably");

    // The principal rides the history rows, read through the flushed store.
    let page = fx
        .history
        .read(
            &human_scope(),
            rakka_agent::AgentTaskHistoryCursor::start().with_limit(64),
        )
        .await
        .expect("the history reads");
    let accepted_row = page
        .entries
        .iter()
        .find(|entry| entry.kind == AgentTaskHistoryKind::ResultAccepted)
        .expect("the acceptance is history");
    assert_eq!(accepted_row.principal.as_deref(), Some(PRINCIPAL));

    // One accepted decision, counted once — the duplicate counted nothing.
    let observations = metrics.snapshot();
    let human_results = observations.observations_named(METRIC_AGENT_HUMAN_RESULTS);
    assert_eq!(human_results.len(), 1, "one durable decision, one count");
}

/// A past-window replay of the accepted submission converges on the recorded
/// result through the durable echo, before the terminal guard.
#[tokio::test]
async fn a_past_window_accepted_replay_converges_on_the_recorded_decision() {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    create_human_task(&fx, Vec::new()).await;
    fx.apply_task_command_at(
        &human_scope(),
        submit_command("submit-1", json!({ "answer": "ok" })),
    )
    .await
    .expect("the submission applies");

    // Age the operation log past its bounded window with distinct refused
    // submissions: every refusal is non-committing, so nothing but the log
    // itself moves.
    for index in 0..rakka_agent::AGENT_TASK_OPERATION_LOG_CAPACITY {
        let _ = fx
            .apply_task_command_at(
                &human_scope(),
                submit_command(&format!("age-{index}"), json!({ "answer": "late" })),
            )
            .await;
    }

    // The replay is answered from the accepted-result echo: applied
    // idempotently, no second transition, the recorded digest returned.
    let replay = fx
        .apply_task_command_at(
            &human_scope(),
            submit_command("submit-1", json!({ "answer": "different" })),
        )
        .await
        .expect("the replay converges");
    let decision = applied_submission(&replay);
    assert_eq!(
        decision.disposition,
        AgentTaskSubmissionDisposition::Accepted
    );
    let state = human_state(&fx).await;
    let accepted = state
        .task()
        .expect("the task exists")
        .accepted_result
        .as_deref()
        .expect("the result stands")
        .clone();
    assert_eq!(
        decision.digest, accepted.digest,
        "first writer wins; the replay's content was never re-read"
    );
    let page = fx
        .history
        .read(
            &human_scope(),
            rakka_agent::AgentTaskHistoryCursor::start().with_limit(64),
        )
        .await
        .expect("the history reads");
    assert_eq!(
        page.entries
            .iter()
            .filter(|entry| entry.kind == AgentTaskHistoryKind::ResultAccepted)
            .count(),
        1,
        "one acceptance, ever"
    );
}

/// Scenario 41's failure half: exhausting the rejection budget fails the
/// human task, and the failure propagates the declared policy over a LIVE
/// dependent run through the cancellation request, never `terminate`.
#[tokio::test]
async fn an_exhausted_human_task_cancels_its_live_dependent_through_the_request_path() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
    .with_metrics(metrics.clone());
    fx.instantiate_agent().await;
    create_human_task(&fx, Vec::new()).await;

    // The dependent is created independent and its assignment accepted — a
    // live run — before the human edge is declared late.
    fx.create_task().await;
    assert_eq!(fx.task_snapshot().await.status, AgentTaskStatus::InProgress);
    fx.apply_task_command(AgentTaskEntityCommand::DeclareDependency {
        operation_id: AgentOperationId::new(
            rakka_agent::AgentOperationKind::Command,
            [TENANT, TASK, "declare-human-edge"],
        )
        .expect("the operation id derives"),
        declaration: Box::new(AgentTaskDependencyDeclaration::new(
            human_scope().task().clone(),
        )),
    })
    .await
    .expect("the edge declares");
    settle_pair(&fx).await;
    assert!(
        human_state(&fx)
            .await
            .task()
            .expect("the human task exists")
            .dependents
            .contains_key(task_scope().task()),
        "the live dependent registered"
    );

    // Three malformed submissions exhaust the default rejection budget; each
    // is a durable decision with decreasing headroom, and the third fails
    // the task rather than silently accepting it.
    for (index, expected_remaining) in [(0_u32, 2_u32), (1, 1), (2, 0)] {
        let reply = fx
            .apply_task_command_at(
                &human_scope(),
                submit_command(&format!("bad-{index}"), json!({ "answer": "" })),
            )
            .await
            .expect("the rejection commits");
        let decision = applied_submission(&reply);
        assert_eq!(
            decision.disposition,
            AgentTaskSubmissionDisposition::Rejected
        );
        assert_eq!(decision.remaining_attempts, expected_remaining);
    }
    let human = human_state(&fx).await;
    assert_eq!(
        human.task().expect("the human task exists").status,
        AgentTaskStatus::Failed
    );

    // The failure reaches the dependent as a request: the marker is set, the
    // run receives its cancel, and the task stays nonterminal until the
    // subtree quiesces — never a direct terminalization over a live run.
    fx.settle_task_at(&human_scope())
        .await
        .expect("the outcome notification delivers");
    let dependent = load_agent_task_state(&fx.tasks, &task_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the dependent exists");
    let dependent_task = dependent.task().expect("the dependent exists");
    assert!(
        dependent_task.cancellation.is_some(),
        "the failure requested cancellation"
    );
    assert!(
        !dependent_task.status.is_terminal(),
        "a live run holds the dependent nonterminal until it winds down"
    );

    // Wind the world down: the run cancels, settles its ledger, and the
    // closed escrow finalizes the dependent `Cancelled` under the
    // dependency reason.
    for _round in 0..8 {
        let _ = fx.settle_task_at(&task_scope()).await;
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        let _ = run.settle_side_effects(&fx.router, fx.now()).await;
        let _ = fx.dispatcher.drive(&mut run, &fx.router, fx.now()).await;
    }
    let dependent = fx.task_snapshot().await;
    assert_eq!(dependent.status, AgentTaskStatus::Cancelled);
    assert_eq!(
        dependent
            .terminal_reason
            .as_ref()
            .map(rakka_agent::AgentTaskTerminalReason::code),
        Some("dependency-not-satisfied")
    );

    // The decisions counted once each: two rejections, one exhaustion.
    let observations = metrics.snapshot();
    let human_results = observations.observations_named(METRIC_AGENT_HUMAN_RESULTS);
    assert_eq!(human_results.len(), 3);
}

/// The fence ladder's refusals are non-committing and spelled exactly.
#[tokio::test]
async fn the_submission_ladder_refuses_without_committing() {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    fx.instantiate_agent().await;

    // No task yet.
    let missing = fx
        .apply_task_command_at(
            &human_scope(),
            submit_command("early", json!({ "answer": "x" })),
        )
        .await
        .expect_err("an uncreated task refuses");
    assert_eq!(missing.code(), "task-not-created");

    // An agent-owned target never accepts a human submission.
    fx.create_task().await;
    let not_human = fx
        .apply_task_command(AgentTaskEntityCommand::SubmitHumanResult {
            operation_id: human_result_operation_id(&tenant(), task_scope().task(), "wrong-door")
                .expect("the operation id derives"),
            submission: Box::new(submission("wrong-door", json!({ "answer": "x" }))),
        })
        .await
        .expect_err("an agent-owned task refuses");
    assert_eq!(not_human.code(), "task-not-human-owned");

    // A human task still blocked on a dependency cannot complete early.
    create_human_task(
        &fx,
        vec![AgentTaskDependencyDeclaration::new(
            AgentTaskId::new("upstream-x").expect("the id is valid"),
        )],
    )
    .await;
    let blocked = fx
        .apply_task_command_at(
            &human_scope(),
            submit_command("blocked", json!({ "answer": "x" })),
        )
        .await
        .expect_err("a blocked task refuses");
    assert_eq!(
        blocked.code(),
        "task-dependencies-unresolved",
        "the dependency gate is re-tested against the edges themselves, not the status          they project, so it names the real reason"
    );

    // Unblock it, then refuse the malformed principals.
    fx.apply_task_command_at(
        &human_scope(),
        AgentTaskEntityCommand::RecordDependencyOutcome {
            operation_id: AgentOperationId::new(
                rakka_agent::AgentOperationKind::Command,
                [TENANT, HUMAN_TASK, "unblock"],
            )
            .expect("the operation id derives"),
            dependency: AgentTaskId::new("upstream-x").expect("the id is valid"),
            outcome: AgentTaskDependencyOutcome::Completed,
        },
    )
    .await
    .expect("the edge resolves");

    let mut no_principal = submission("anon", json!({ "answer": "x" }));
    no_principal.principal = String::new();
    let refused = fx
        .apply_task_command_at(
            &human_scope(),
            AgentTaskEntityCommand::SubmitHumanResult {
                operation_id: human_result_operation_id(&tenant(), human_scope().task(), "anon")
                    .expect("the operation id derives"),
                submission: Box::new(no_principal),
            },
        )
        .await
        .expect_err("an unauthenticated submission refuses");
    assert_eq!(refused.code(), "submission-principal-missing");

    let mut oversized = submission("big", json!({ "answer": "x" }));
    oversized.principal = "p".repeat(rakka_agent::AGENT_IDENTITY_MAX_LENGTH + 1);
    let refused = fx
        .apply_task_command_at(
            &human_scope(),
            AgentTaskEntityCommand::SubmitHumanResult {
                operation_id: human_result_operation_id(&tenant(), human_scope().task(), "big")
                    .expect("the operation id derives"),
                submission: Box::new(oversized),
            },
        )
        .await
        .expect_err("an oversized principal refuses");
    assert_eq!(refused.code(), "submission-principal-too-long");

    // A mismatched definition claim is a committed rejection, not a refusal
    // — the same deterministic decision a run's proposal gets.
    let mut mismatched = submission("stale", json!({ "answer": "x" }));
    mismatched.definition_version = AgentRevisionNumber::new(9);
    let reply = fx
        .apply_task_command_at(
            &human_scope(),
            AgentTaskEntityCommand::SubmitHumanResult {
                operation_id: human_result_operation_id(&tenant(), human_scope().task(), "stale")
                    .expect("the operation id derives"),
                submission: Box::new(mismatched),
            },
        )
        .await
        .expect("the rejection commits");
    let decision = applied_submission(&reply);
    assert_eq!(
        decision.code.as_deref(),
        Some("definition-version-mismatch"),
        "the definition fence is the validation core's own"
    );

    // Nothing above committed a result.
    let state = human_state(&fx).await;
    let task = state.task().expect("the task exists");
    assert!(task.accepted_result.is_none());
    assert_eq!(task.status, AgentTaskStatus::WaitingForInput);
    assert_eq!(task.rejection_count, 1, "only the rejection committed");

    // Terminal-by-another-submission refuses honestly.
    fx.apply_task_command_at(
        &human_scope(),
        submit_command("final", json!({ "answer": "done" })),
    )
    .await
    .expect("the acceptance applies");
    let terminal = fx
        .apply_task_command_at(
            &human_scope(),
            submit_command("late", json!({ "answer": "late" })),
        )
        .await
        .expect_err("a terminal task refuses a different submission");
    assert_eq!(terminal.code(), "task-terminal");
}

/// A replayed rejection converges on the recorded decision without spending
/// the budget twice: the materialized echo answers the latest, the bounded
/// ring refuses the older ones.
#[tokio::test]
async fn a_replayed_rejection_never_spends_the_budget_twice() {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    // The largest budget a definition may declare — which is exactly the
    // echo ring's capacity, so every rejection inside the budget is covered
    // by the ring — and far more than the two this test spends, so the
    // ladder rather than exhaustion is what it observes.
    let definition = task_definition()
        .with_ownership(AgentTaskOwnership::Human)
        .with_limits(rakka_agent::AgentTaskLimits {
            max_result_rejections: AGENT_TASK_REJECTED_SUBMISSION_ECHO_CAPACITY as u32,
            ..task_definition().limits
        });
    fx.apply_task_command_at(
        &human_scope(),
        AgentTaskEntityCommand::Create {
            operation_id: creation_op(HUMAN_TASK),
            creation: Box::new(AgentTaskCreation {
                definition,
                input: AgentTaskContent::inline(json!({ "ticket": 9 }))
                    .expect("the input is inline-bounded"),
                assignee: None,
                team: None,
                goal: None,
                goal_mode: Default::default(),
                goal_spec: None,
                parent: None,
                dependencies: Vec::new(),
                escrow: None,
                wake: None,
                delegation: None,
                telemetry: Default::default(),
            }),
        },
    )
    .await
    .expect("the human task creates");

    fx.apply_task_command_at(
        &human_scope(),
        submit_command("bad-1", json!({ "answer": "" })),
    )
    .await
    .expect("the first rejection commits");

    // In-window replay: answered from the operation log.
    let replay = fx
        .apply_task_command_at(
            &human_scope(),
            submit_command("bad-1", json!({ "answer": "" })),
        )
        .await
        .expect("the replay answers");
    assert!(matches!(replay, AgentTaskEntityReply::Duplicate { .. }));

    // A second rejection makes `bad-1` the *older* decision; then age the
    // operation log out entirely with committed transitions that leave the
    // result cells untouched — a refused submission would not do, because a
    // non-committing refusal never enters the log. Thirty-two declared
    // edges and their thirty-two resolutions are exactly the window.
    fx.apply_task_command_at(
        &human_scope(),
        submit_command("bad-2", json!({ "answer": "" })),
    )
    .await
    .expect("the second rejection commits");
    for index in 0..rakka_agent::AGENT_TASK_MAX_DEPENDENCIES {
        let upstream = AgentTaskId::new(format!("age-up-{index}")).expect("the id is valid");
        fx.apply_task_command_at(
            &human_scope(),
            AgentTaskEntityCommand::DeclareDependency {
                operation_id: AgentOperationId::new(
                    rakka_agent::AgentOperationKind::Command,
                    [TENANT, HUMAN_TASK, &format!("age-declare-{index}")],
                )
                .expect("the operation id derives"),
                declaration: Box::new(AgentTaskDependencyDeclaration::new(upstream.clone())),
            },
        )
        .await
        .expect("the ager edge declares");
        fx.apply_task_command_at(
            &human_scope(),
            AgentTaskEntityCommand::RecordDependencyOutcome {
                operation_id: AgentOperationId::new(
                    rakka_agent::AgentOperationKind::Command,
                    [TENANT, HUMAN_TASK, &format!("age-resolve-{index}")],
                )
                .expect("the operation id derives"),
                dependency: upstream,
                outcome: AgentTaskDependencyOutcome::Completed,
            },
        )
        .await
        .expect("the ager edge resolves");
    }

    // Past-window replay of the LATEST rejection: echoed from the
    // materialized record, budget untouched — and answered with no
    // transition at all.
    //
    // Writing nothing is the load-bearing part, not an optimization. The
    // entity runs its history-headroom guard before any transition, so an
    // echo answered *inside* one would be refused `task-history-backlog`
    // while the history sink — pure observability — is backed up, telling a
    // caller whose submission already decided the task that nothing
    // happened. The echo is answered before that guard precisely because it
    // needs nothing the guard protects.
    let quiesced_at = human_state(&fx).await.updated_at();
    let echoed = fx
        .apply_task_command_at(
            &human_scope(),
            submit_command("bad-2", json!({ "answer": "" })),
        )
        .await
        .expect("the latest rejection echoes");
    assert!(
        matches!(echoed, AgentTaskEntityReply::Duplicate { .. }),
        "a past-window echo answers from the record; it does not re-transition"
    );
    assert_eq!(
        human_state(&fx).await.updated_at(),
        quiesced_at,
        "the echo wrote nothing at all"
    );
    let decision = applied_submission(&echoed);
    assert_eq!(
        decision.disposition,
        AgentTaskSubmissionDisposition::Rejected
    );

    // Past-window replay of the OLDER rejection: refused from the bounded
    // ring, budget untouched.
    let older = fx
        .apply_task_command_at(
            &human_scope(),
            submit_command("bad-1", json!({ "answer": "" })),
        )
        .await
        .expect_err("the older rejection refuses");
    assert_eq!(older.code(), "submission-already-rejected");

    let state = human_state(&fx).await;
    assert_eq!(
        state.task().expect("the task exists").rejection_count,
        2,
        "replays spent nothing"
    );
}

/// Owner loss at every durable write of the full happy path converges on one
/// accepted result, one resolved edge, one unblocked dependent.
#[tokio::test]
async fn the_completion_flow_survives_any_owner_loss() {
    use rakka_agent::testkit::CrashPoint;

    async fn world() -> Fixture {
        let fx = Fixture::new(ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new(),
        ));
        fx.instantiate_agent().await;
        create_human_task(&fx, Vec::new()).await;
        create_dependent_on_human(&fx).await;
        fx
    }

    async fn drive(fx: &Fixture) {
        let _ = fx
            .apply_task_command_at(
                &human_scope(),
                submit_command("submit-1", json!({ "answer": "ok" })),
            )
            .await;
        for _round in 0..4 {
            let _ = fx.settle_task_at(&human_scope()).await;
            let _ = fx.settle_task_at(&task_scope()).await;
        }
    }

    async fn assert_converged(fx: &Fixture) {
        let _ = fx
            .apply_task_command_at(
                &human_scope(),
                submit_command("submit-1", json!({ "answer": "ok" })),
            )
            .await;
        for _round in 0..6 {
            let _ = fx.settle_task_at(&human_scope()).await;
            let _ = fx.settle_task_at(&task_scope()).await;
        }
        let human = load_agent_task_state(&fx.tasks, &human_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the state loads")
            .expect("the task exists");
        let human_task = human.task().expect("the human task exists");
        assert_eq!(human_task.status, AgentTaskStatus::Completed);
        assert!(human_task.accepted_result.is_some());
        assert!(
            human_task
                .dependents
                .get(task_scope().task())
                .is_some_and(|record| record.outcome_settled),
            "the notification settled exactly once"
        );
        let dependent = fx.task_snapshot().await;
        assert_ne!(dependent.status, AgentTaskStatus::Blocked);
        assert!(dependent.dependencies_satisfied);
    }

    // Reference run counts the writes the flow attempts.
    let reference = world().await;
    reference.tasks.reset_writes();
    drive(&reference).await;
    let writes = reference.tasks.writes();
    assert!(
        writes >= 3,
        "the flow writes the task store at least thrice (submission, registration, outcome)"
    );

    for point in 1..=writes {
        for window in [CrashPoint::BeforeWrite, CrashPoint::AfterWrite] {
            let fx = world().await;
            fx.tasks.reset_writes();
            fx.tasks.crash_at(point, window);
            drive(&fx).await;
            fx.tasks.assert_crash_fired(point, window);
            fx.tasks.survive();
            assert_converged(&fx).await;
        }
    }
}

/// The checkpoint boundary stays checkpoint-bound: nothing on the human path
/// touches effect gates, and the type-level statement is pinned here so the
/// boundary survives refactors. A human submission binds a task result;
/// resolving an effect-bound approval remains `AgentCheckpoint`'s alone
/// ([specification 8.12](../../../docs/plans/rakka-agent/spec.md)).
#[tokio::test]
async fn a_human_result_never_resolves_a_checkpoint() {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    create_human_task(&fx, Vec::new()).await;
    fx.apply_task_command_at(
        &human_scope(),
        submit_command("submit-1", json!({ "answer": "ok" })),
    )
    .await
    .expect("the submission applies");
    // The acceptance produced a task result and nothing else: no checkpoint
    // store exists on this path to resolve, and the digest recorded is the
    // content fingerprint, not a grant binding.
    let state = human_state(&fx).await;
    let accepted = state
        .task()
        .expect("the task exists")
        .accepted_result
        .as_deref()
        .expect("the result stands")
        .clone();
    assert_eq!(
        accepted.digest,
        AgentContentDigest::of_json(&json!({ "answer": "ok" })),
        "the digest is the content fingerprint the validation recorded"
    );
}

/// A submission against a cancel-marked human task is refused with the
/// stable cancellation code.
#[tokio::test]
async fn a_cancel_requested_task_refuses_submissions() {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    // A dependency edge keeps the request path from finalizing instantly:
    // an unresolved registration holds nothing, so instead the human task is
    // cancelled directly and the refusal observed between request and
    // finalization. With no escrow the request finalizes in its own
    // transition, so the refusal here reads `task-terminal` — the honest
    // post-finalization answer — unless a submission races the request.
    create_human_task(&fx, Vec::new()).await;
    fx.apply_task_command_at(
        &human_scope(),
        AgentTaskEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, HUMAN_TASK, "operator"],
            )
            .expect("the operation id derives"),
            reason: "operator-cancelled".to_string(),
        },
    )
    .await
    .expect("the cancellation applies");
    let refused = fx
        .apply_task_command_at(
            &human_scope(),
            submit_command("late", json!({ "answer": "x" })),
        )
        .await
        .expect_err("the cancelled task refuses");
    // A human task holds no escrow, so the request finalized immediately.
    assert_eq!(refused.code(), "task-terminal");
    match refused {
        AgentTaskError::Terminal { status } => assert_eq!(status, AgentTaskStatus::Cancelled),
        other => panic!("unexpected refusal: {other:?}"),
    }
}

/// A dependency declared *after* creation closes the submission door, and
/// resolving it reopens it.
///
/// A human-owned task with no dependencies is born `WaitingForInput`, which
/// is exactly what the submission door reads as "the graph permits this now".
/// A late declaration has to close that door, and the door re-tests the edges
/// themselves rather than trusting the status to have been demoted.
#[tokio::test]
async fn a_late_declared_dependency_closes_the_submission_door() {
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ));
    create_human_task(&fx, Vec::new()).await;
    assert_eq!(
        human_state(&fx)
            .await
            .task()
            .expect("the task exists")
            .status,
        AgentTaskStatus::WaitingForInput
    );

    let upstream = AgentTaskId::new("late-upstream").expect("the id is valid");
    fx.apply_task_command_at(
        &human_scope(),
        AgentTaskEntityCommand::DeclareDependency {
            operation_id: AgentOperationId::new(
                rakka_agent::AgentOperationKind::Command,
                [TENANT, HUMAN_TASK, "late-declare"],
            )
            .expect("the operation id derives"),
            declaration: Box::new(AgentTaskDependencyDeclaration::new(upstream.clone())),
        },
    )
    .await
    .expect("the late edge declares");

    assert_eq!(
        human_state(&fx)
            .await
            .task()
            .expect("the task exists")
            .status,
        AgentTaskStatus::Blocked,
        "the late edge demotes the human task exactly as it does an agent-owned one"
    );
    let refused = fx
        .apply_task_command_at(
            &human_scope(),
            submit_command("too-early", json!({ "answer": "approved" })),
        )
        .await
        .expect_err("the declared input has not resolved");
    assert_eq!(refused.code(), "task-dependencies-unresolved");
    assert!(
        human_state(&fx)
            .await
            .task()
            .expect("the task exists")
            .accepted_result
            .is_none(),
        "the refusal committed nothing"
    );

    // Resolving the edge reopens the door.
    fx.apply_task_command_at(
        &human_scope(),
        AgentTaskEntityCommand::RecordDependencyOutcome {
            operation_id: AgentOperationId::new(
                rakka_agent::AgentOperationKind::Command,
                [TENANT, HUMAN_TASK, "late-resolve"],
            )
            .expect("the operation id derives"),
            dependency: upstream,
            outcome: AgentTaskDependencyOutcome::Completed,
        },
    )
    .await
    .expect("the edge resolves");
    fx.apply_task_command_at(
        &human_scope(),
        submit_command("now-ok", json!({ "answer": "approved" })),
    )
    .await
    .expect("the submission is admitted once its input exists");
    assert_eq!(
        human_state(&fx)
            .await
            .task()
            .expect("the task exists")
            .status,
        AgentTaskStatus::Completed
    );
}

/// Two definitions that could only ever fail are refused where they are
/// declared, rather than discovered when a submission is spent against them.
#[tokio::test]
async fn an_unsatisfiable_definition_is_refused_at_declaration() {
    // A rejection budget larger than the echo ring that guards it: the ring
    // would evict a live rejection, and the replay would re-spend it.
    let over_budget = task_definition()
        .with_ownership(AgentTaskOwnership::Human)
        .with_limits(rakka_agent::AgentTaskLimits {
            max_result_rejections: AGENT_TASK_REJECTED_SUBMISSION_ECHO_CAPACITY as u32 + 1,
            ..task_definition().limits
        });
    let error = over_budget
        .validate()
        .expect_err("a budget past the echo ring is refused");
    assert_eq!(error.code(), "invalid-task-definition");
    assert!(
        task_definition()
            .with_ownership(AgentTaskOwnership::Human)
            .with_limits(rakka_agent::AgentTaskLimits {
                max_result_rejections: AGENT_TASK_REJECTED_SUBMISSION_ECHO_CAPACITY as u32,
                ..task_definition().limits
            })
            .validate()
            .is_ok(),
        "a budget the ring fully covers stands"
    );

    // A human-owned task requiring evidence: no submission surface carries
    // evidence artifacts, so every attempt would be a committed rejection
    // walking the task to exhaustion — and taking its dependents with it.
    let requires_evidence = task_definition()
        .with_ownership(AgentTaskOwnership::Human)
        .with_result_rule(rakka_agent::AgentTaskResultRule::new(
            rakka_agent::AgentTaskRuleId::new("evidence").expect("the rule id is valid"),
            rakka_agent::AgentTaskResultCheck::EvidenceRequired,
        ));
    let error = requires_evidence
        .validate()
        .expect_err("an unsatisfiable human rule is refused");
    assert_eq!(error.code(), "invalid-task-definition");
    assert!(
        task_definition()
            .with_result_rule(rakka_agent::AgentTaskResultRule::new(
                rakka_agent::AgentTaskRuleId::new("evidence").expect("the rule id is valid"),
                rakka_agent::AgentTaskResultCheck::EvidenceRequired,
            ))
            .validate()
            .is_ok(),
        "an agent-owned task may still require evidence: its run can carry artifacts"
    );
}
