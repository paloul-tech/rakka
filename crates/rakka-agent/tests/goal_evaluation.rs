//! Goal evaluation as a durable effect, and the attested decision door.
//!
//! Specification: section 8.3 — an agent or model may propose that a goal is
//! complete, but only the configured evaluator's assessment of the current
//! success-criteria revision against durable evidence makes it `Satisfied`
//! (scenario 30). Every entity here is rebuilt from durable state per call —
//! the `Fixture` convention — so every step already arrives after a restart.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    evaluation_operation_id, AgentDispatchFuture, AgentGoalDecision, AgentGoalEvaluationExecutor,
    AgentGoalEvaluationFinding, AgentGoalEvaluationMethod, AgentGoalEvaluationMethodKind,
    AgentGoalEvaluationOutcome, AgentGoalEvaluationRequest, AgentGoalEvidenceRef, AgentGoalStatus,
    AgentGoalTerminalReason, AgentModelTurn, AgentModelUsage, AgentOperationId, AgentOperationKind,
    AgentPolicyRef, AgentRevisionNumber, AgentRunEffect, AgentRunEffectKind, AgentRunEffectRequest,
    AgentRunEntityCommand, AgentRunEntityReply, AgentRunScope, AgentRunStatus,
    AgentTaskEntityCommand, AgentTaskEntityReply, AGENT_GOAL_EVALUATION_HUMAN_DECISION_CLASS,
    AGENT_GOAL_EVALUATION_MAX_EVIDENCE, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentEphemeralCredential, AgentTimestampMillis, PrincipalRef};

mod common;

use common::{
    agent_scope, goal_evaluation, goal_evaluation_request, goal_spec, goal_spec_draft,
    goal_spec_with_evaluator, goal_task_creation_command, provenance, run_scope, task_definition,
    Fixture, TASK, TENANT,
};

fn text_turn(text: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(text)
        .with_usage(AgentModelUsage {
            input_tokens: 8,
            output_tokens: 4,
            cost_micros: 2,
        })
}

/// A scripted application-owned evaluator: it judges as asked, echoing the
/// request's classed evidence back as the verdict's.
struct ScriptedEvaluator {
    outcome: AgentGoalEvaluationOutcome,
}

impl AgentGoalEvaluationExecutor for ScriptedEvaluator {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        evaluation: &'a AgentGoalEvaluationRequest,
        _credential: Option<&'a AgentEphemeralCredential>,
        _now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentGoalEvaluationFinding> {
        Box::pin(async move {
            Ok(AgentGoalEvaluationFinding::Evaluated {
                outcome: self.outcome,
                reason_code: "scripted".to_string(),
                evidence: evaluation.evidence.clone(),
                evaluated_by: None,
            })
        })
    }
}

/// The fixture world: one scripted model turn so the coordinator run stays
/// live, and — when given — the wired evaluation executor.
fn fixture_with(executor: Option<Arc<dyn AgentGoalEvaluationExecutor>>) -> Fixture {
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn_for(1, text_turn("investigating the ticket")),
    );
    let dispatcher = match executor {
        Some(executor) => dispatcher.with_goal_evaluation_executor(executor),
        None => dispatcher,
    };
    Fixture::new(dispatcher)
}

fn satisfying_fixture() -> Fixture {
    fixture_with(Some(Arc::new(ScriptedEvaluator {
        outcome: AgentGoalEvaluationOutcome::Satisfied,
    })))
}

/// Creates the goal-bearing root task; its assignment and the run's
/// acceptance settle inside the creating command's own drive.
async fn create_goal_task(fx: &Fixture, spec: rakka_agent::AgentGoalSpec) {
    fx.instantiate_agent().await;
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the goal-bearing creation applies");
}

fn evaluate_command(
    discriminator: &str,
    request: AgentGoalEvaluationRequest,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::EvaluateGoal {
        operation_id: evaluation_operation_id(&run_scope(), discriminator)
            .expect("the operation id derives"),
        evaluation: Box::new(request),
    }
}

async fn apply_run(
    fx: &Fixture,
    command: AgentRunEntityCommand,
) -> Result<AgentRunEntityReply, rakka_agent::AgentRunError> {
    let mut run = fx.run();
    run.recover(fx.now()).await?;
    run.apply(command, &fx.router, fx.now()).await
}

/// The outstanding evaluation effect the run holds.
async fn evaluation_effect(fx: &Fixture) -> AgentRunEffect {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.state()
        .expect("the state reads")
        .loop_state()
        .expect("the loop state exists")
        .effects()
        .iter()
        .find(|effect| {
            effect.kind() == AgentRunEffectKind::GoalEvaluationCall && effect.is_outstanding()
        })
        .cloned()
        .expect("an evaluation effect is outstanding")
}

/// Answers the outstanding evaluation effect exactly as the dispatcher would
/// — the executor, or the effect-bound grant for a human review — and applies
/// its result. The applying transition's settle pass owes, couriers, and
/// settles the goal-evaluation exchange in the same sweep.
async fn answer_evaluation(fx: &Fixture) -> AgentRunEntityReply {
    let effect = evaluation_effect(fx).await;
    let AgentRunEffectRequest::Evaluation { evaluation } = &effect.request else {
        panic!("the effect carries an evaluation request");
    };
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let grant = run
        .state()
        .expect("the state reads")
        .loop_state()
        .expect("the loop state exists")
        .grant_for(&effect)
        .cloned();
    let outcome = fx
        .dispatcher
        .evaluation_outcome(&run_scope(), &effect, evaluation, grant.as_ref(), fx.now())
        .await;
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(&run_scope())
                .expect("the result operation id derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: 1,
            fence: 0,
            outcome: Box::new(outcome),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the evaluation result applies")
}

async fn goal_view(fx: &Fixture) -> rakka_agent::AgentGoalStatusView {
    fx.task_snapshot()
        .await
        .goal_state
        .expect("the goal view exists")
}

async fn run_status(fx: &Fixture) -> AgentRunStatus {
    fx.run_snapshot().await.expect("the run exists").status
}

/// The run's durable loop state, read exactly as recovery reads it.
async fn run_loop_state(fx: &Fixture) -> rakka_agent::AgentLoopState {
    rakka_agent::load_agent_run_state(
        &fx.runs,
        &run_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run state exists")
    .loop_state()
    .expect("the loop state exists")
    .clone()
}

fn operation(step: &str) -> AgentOperationId {
    AgentOperationId::new(AgentOperationKind::Command, [TENANT, TASK, step])
        .expect("the operation id derives")
}

/// `count` classed evidence items, the first covering the fixture spec's
/// required `artifact` class so only the *bound* is under test.
fn evidence_filling(count: usize) -> Vec<AgentGoalEvidenceRef> {
    (0..count)
        .map(|index| AgentGoalEvidenceRef {
            class: if index == 0 {
                "artifact".to_string()
            } else {
                format!("filler-{index:02}")
            },
            artifact: None,
            digest: None,
        })
        .collect()
}

/// Approves the one open evaluation checkpoint as an authorized human.
async fn approve_open_review(fx: &Fixture) {
    let effect = evaluation_effect(fx).await;
    let checkpoint = run_loop_state(fx)
        .await
        .open_checkpoints()
        .iter()
        .find(|checkpoint| checkpoint.bound_effect.effect_id == effect.effect_id)
        .expect("the approval checkpoint is open")
        .checkpoint_id
        .clone();
    apply_run(
        fx,
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                "bounded-review",
            )
            .expect("the decision key derives"),
            checkpoint_id: checkpoint,
            resolver: PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "goal-approver".to_string(),
                display_name: None,
            },
            decision: Box::new(rakka_agent::AgentCheckpointDecision::Approval(
                rakka_agent::AgentApprovalDecision::Approve {
                    credential_binding: None,
                    expires_at: AgentTimestampMillis::new(1_000_000),
                    allowed_use_count: 1,
                },
            )),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
    )
    .await
    .expect("the approval applies");
}

#[tokio::test]
async fn scenario_30_satisfied_only_via_evaluation_of_current_revision_against_durable_evidence() {
    let fx = satisfying_fixture();
    create_goal_task(&fx, goal_spec_with_evaluator()).await;

    // Work alone — an accepted assignment, a live coordinator — moves the
    // goal nowhere.
    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Active);

    // A declaration is not an evaluation: under the configured evaluator, the
    // open command cannot make a criteria decision at all, however complete
    // its reference looks.
    let declared = fx
        .apply_task_command(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: operation("declare"),
            decision: Box::new(AgentGoalDecision {
                reason: AgentGoalTerminalReason::CriteriaSatisfied,
                evaluation: Some(Box::new(goal_evaluation())),
                provenance: Some(Box::new(provenance(50))),
                expected_status_revision: AgentRevisionNumber::INITIAL,
            }),
        })
        .await;
    assert_eq!(
        declared.expect_err("the declaration is refused").code(),
        "task-goal-decision-unattested"
    );
    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Active);

    // The coordinator commits the evaluation as a durable effect; the
    // executor judges it; the record crosses the exchange; the door decides.
    let reply = apply_run(
        &fx,
        evaluate_command("1", goal_evaluation_request(AgentRevisionNumber::INITIAL)),
    )
    .await
    .expect("the evaluation commits");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    answer_evaluation(&fx).await;

    let goal = goal_view(&fx).await;
    assert_eq!(goal.status, AgentGoalStatus::Satisfied);
    assert_eq!(
        goal.terminal,
        Some(AgentGoalTerminalReason::CriteriaSatisfied)
    );
    // The run outlives the decision: evaluation is not the turn, and the
    // coordinator keeps working until its own conclusion.
    assert!(!run_status(&fx).await.is_terminal());

    // The persisted decision names the whole attestation: the configured
    // evaluator, the criteria revision in force, the classed evidence
    // covering the required class, the record's identity, and its
    // cryptographic digest.
    let state = rakka_agent::load_agent_task_state(
        &fx.tasks,
        &common::task_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the state loads")
    .expect("the state exists");
    let goal_record = state
        .task()
        .expect("the task exists")
        .goal_state
        .as_deref()
        .expect("the goal record exists");
    let terminal = goal_record.terminal().expect("the decision persists");
    let evaluation = terminal
        .evaluation
        .clone()
        .expect("the decision carries the evaluation");
    assert_eq!(
        evaluation.evaluator,
        AgentPolicyRef::new("ticket-evaluator").expect("the policy ref is valid")
    );
    assert_eq!(evaluation.criteria_revision, AgentRevisionNumber::INITIAL);
    assert!(
        evaluation
            .evidence_items
            .iter()
            .any(|item| item.class == "artifact"),
        "the required class is covered"
    );
    assert_eq!(
        evaluation.method,
        Some(AgentGoalEvaluationMethodKind::DeterministicAssertion)
    );
    assert!(evaluation.evaluation_id.is_some(), "the record is named");
    let digest = evaluation.digest.expect("the attestation digest binds");
    assert!(
        digest.algorithm.is_cryptographic(),
        "sha-256, never the fingerprint"
    );

    // Re-sweeping the settled world decides nothing twice.
    let revision = goal_view(&fx).await.status_revision;
    fx.pump().await.expect("the settled world stays settled");
    assert_eq!(goal_view(&fx).await.status_revision, revision);
}

#[tokio::test]
async fn a_not_satisfied_verdict_ends_the_goal_unsatisfied() {
    // Both verdicts are terminal. `NotSatisfied` is the evaluator's
    // conclusive "the criteria are not met", not a progress report, and it
    // ends the contract `Unsatisfied` through the same attested door a
    // satisfying verdict uses. An evaluator that means "not met *yet*" refuses
    // instead — `a_failed_evaluation_leaves_the_run_and_goal_live` pins that
    // path, and the two together are the whole outcome surface.
    let fx = fixture_with(Some(Arc::new(ScriptedEvaluator {
        outcome: AgentGoalEvaluationOutcome::NotSatisfied,
    })));
    create_goal_task(&fx, goal_spec_with_evaluator()).await;

    apply_run(
        &fx,
        evaluate_command("1", goal_evaluation_request(AgentRevisionNumber::INITIAL)),
    )
    .await
    .expect("the evaluation commits");
    answer_evaluation(&fx).await;

    let goal = goal_view(&fx).await;
    assert_eq!(goal.status, AgentGoalStatus::Unsatisfied);
    assert_eq!(goal.terminal, Some(AgentGoalTerminalReason::CriteriaNotMet));

    // The door *accepted* it — an unsatisfying verdict is a decision, not a
    // refusal — so the cell settled clean, with the verdict on the record.
    let loop_state = run_loop_state(&fx).await;
    let cell = loop_state
        .goal_evaluation()
        .expect("the cell holds the record");
    assert!(cell.reported, "the exchange settled");
    assert_eq!(cell.refusal, None, "the door accepted the verdict");
    assert_eq!(
        cell.record.outcome,
        AgentGoalEvaluationOutcome::NotSatisfied
    );

    // Terminal and absorbing: a second evaluation cannot reopen the contract,
    // and the run learns so from the door rather than by crashing.
    apply_run(
        &fx,
        evaluate_command("2", goal_evaluation_request(AgentRevisionNumber::INITIAL)),
    )
    .await
    .expect("the second evaluation commits");
    answer_evaluation(&fx).await;
    let loop_state = run_loop_state(&fx).await;
    assert_eq!(
        loop_state
            .goal_evaluation()
            .expect("the cell holds the second report")
            .refusal
            .as_deref(),
        Some("goal-terminal")
    );
    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Unsatisfied);
}

#[tokio::test]
async fn an_expired_goal_refuses_as_terminal_rather_than_unattested() {
    // Two fences meet on a commanded criteria decision past the deadline: the
    // attestation fence, and the expiry every goal entry point observes. The
    // honest one must win — the decision is not unattested, the goal is over —
    // so the deadline is observed *before* the attestation check and `decide`
    // answers `goal-terminal`.
    let fx = satisfying_fixture();
    let mut spec = goal_spec_with_evaluator();
    spec.deadline = Some(AgentTimestampMillis::new(40_000));
    create_goal_task(&fx, spec).await;
    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Active);

    fx.clock.store(60_000, Ordering::SeqCst);
    let refused = fx
        .apply_task_command(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: operation("declare-late"),
            decision: Box::new(AgentGoalDecision {
                reason: AgentGoalTerminalReason::CriteriaSatisfied,
                evaluation: Some(Box::new(goal_evaluation())),
                provenance: Some(Box::new(provenance(60))),
                expected_status_revision: AgentRevisionNumber::INITIAL,
            }),
        })
        .await;
    assert_eq!(
        refused.expect_err("an expired goal decides nothing").code(),
        "goal-terminal"
    );

    // The settle pass is what makes the expiry durable — the refused command
    // persisted nothing, exactly as every other refused decision does.
    fx.pump().await.expect("the settle pass runs");
    let goal = goal_view(&fx).await;
    assert_eq!(goal.status, AgentGoalStatus::Expired);
    assert_eq!(
        goal.terminal,
        Some(AgentGoalTerminalReason::DeadlineExpired)
    );
}

#[tokio::test]
async fn a_human_review_reserves_the_evidence_slot_its_decision_needs() {
    // The dispatcher appends the authorized decision as one classed evidence
    // item, so a human review may present at most `MAX_EVIDENCE - 1` of its
    // own. The commit door reserves that slot: a request filling the whole
    // bound is refused *before* anyone is asked to approve, instead of being
    // approved and only then failing to build its over-full record — which
    // would spend the grant for nothing.
    let fx = satisfying_fixture();
    create_goal_task(&fx, goal_spec_with_evaluator()).await;

    let mut request = goal_evaluation_request(AgentRevisionNumber::INITIAL);
    request.method = AgentGoalEvaluationMethod::HumanReview;
    request.evidence = evidence_filling(AGENT_GOAL_EVALUATION_MAX_EVIDENCE);
    let refused = apply_run(&fx, evaluate_command("1", request.clone())).await;
    assert_eq!(
        refused
            .expect_err("a full evidence list leaves no room for the decision")
            .code(),
        "run-goal-evaluation-invalid"
    );
    // Nothing was committed, so no checkpoint was opened and no human was
    // asked: the refusal is the whole consequence.
    assert!(
        run_loop_state(&fx).await.open_checkpoints().is_empty(),
        "a refused commit opens no approval"
    );

    // One slot short of the bound is exactly enough. The same review commits,
    // an authorized human approves it, and the appended decision lands the
    // record on the bound rather than over it.
    request.evidence = evidence_filling(AGENT_GOAL_EVALUATION_MAX_EVIDENCE - 1);
    apply_run(&fx, evaluate_command("2", request))
        .await
        .expect("the evaluation commits with room for the decision");
    approve_open_review(&fx).await;
    answer_evaluation(&fx).await;

    let goal = goal_view(&fx).await;
    assert_eq!(goal.status, AgentGoalStatus::Satisfied);
    let loop_state = run_loop_state(&fx).await;
    let cell = loop_state
        .goal_evaluation()
        .expect("the cell holds the record");
    assert_eq!(cell.refusal, None, "the record built and the door accepted");
    assert_eq!(
        cell.record.evidence.len(),
        AGENT_GOAL_EVALUATION_MAX_EVIDENCE,
        "the appended decision fills the reserved slot exactly"
    );
    assert!(
        cell.record
            .evidence
            .iter()
            .any(|item| item.class == AGENT_GOAL_EVALUATION_HUMAN_DECISION_CLASS),
        "the authorized decision is the appended evidence"
    );
}

#[tokio::test]
async fn a_mismatched_evaluator_is_refused_and_the_goal_stays_decidable() {
    let fx = satisfying_fixture();
    create_goal_task(&fx, goal_spec_with_evaluator()).await;

    let mut request = goal_evaluation_request(AgentRevisionNumber::INITIAL);
    request.evaluator = AgentPolicyRef::new("impostor-evaluator").expect("the ref is valid");
    apply_run(&fx, evaluate_command("1", request))
        .await
        .expect("the evaluation commits");
    answer_evaluation(&fx).await;

    // The door refused; the refusal settled on the run's cell, and the goal
    // is untouched.
    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Active);
    let loop_state = run_loop_state(&fx).await;
    let cell = loop_state
        .goal_evaluation()
        .expect("the cell holds the report");
    assert!(cell.reported, "the exchange settled");
    assert_eq!(cell.refusal.as_deref(), Some("goal-evaluator-mismatch"));

    // The goal stays decidable: the honest evaluator satisfies it.
    apply_run(
        &fx,
        evaluate_command("2", goal_evaluation_request(AgentRevisionNumber::INITIAL)),
    )
    .await
    .expect("the corrected evaluation commits");
    answer_evaluation(&fx).await;
    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Satisfied);
}

#[tokio::test]
async fn a_missing_required_evidence_class_refuses_the_decision() {
    let fx = satisfying_fixture();
    create_goal_task(&fx, goal_spec_with_evaluator()).await;

    let mut request = goal_evaluation_request(AgentRevisionNumber::INITIAL);
    request.evidence[0].class = "anecdote".to_string();
    apply_run(&fx, evaluate_command("1", request))
        .await
        .expect("the evaluation commits");
    answer_evaluation(&fx).await;

    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Active);
    let loop_state = run_loop_state(&fx).await;
    let cell = loop_state
        .goal_evaluation()
        .expect("the cell holds the report");
    assert_eq!(cell.refusal.as_deref(), Some("goal-evidence-missing"));
}

#[tokio::test]
async fn a_criteria_revision_change_invalidates_the_in_flight_evaluation() {
    let fx = satisfying_fixture();
    create_goal_task(&fx, goal_spec_with_evaluator()).await;

    // The evaluation commits against the initial revision...
    apply_run(
        &fx,
        evaluate_command("1", goal_evaluation_request(AgentRevisionNumber::INITIAL)),
    )
    .await
    .expect("the evaluation commits");

    // ...and the owner revises the criteria while it is in flight.
    let applied = fx
        .apply_task_command(AgentTaskEntityCommand::ReviseGoalCriteria {
            operation_id: operation("revise"),
            expected_criteria_revision: AgentRevisionNumber::INITIAL,
            source: rakka_agent::AgentGoalCriteriaSource::Policy(
                AgentPolicyRef::new("ticket-resolved-v2").expect("the ref is valid"),
            ),
            digest: None,
            provenance: Box::new(provenance(60)),
        })
        .await
        .expect("the revision applies");
    assert!(matches!(applied, AgentTaskEntityReply::Applied { .. }));
    let revised = goal_view(&fx).await;
    assert_eq!(
        revised.criteria_revision,
        AgentRevisionNumber::INITIAL.next()
    );
    assert_eq!(revised.status, AgentGoalStatus::Active);

    // The stale evaluation completes, arrives, and is refused — the
    // invalidation is the door's existing fence, not new machinery.
    answer_evaluation(&fx).await;
    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Active);
    assert_eq!(
        run_loop_state(&fx)
            .await
            .goal_evaluation()
            .expect("the cell holds the report")
            .refusal
            .as_deref(),
        Some("goal-evaluation-stale")
    );

    // Re-evaluated at the revision now in force, the goal satisfies.
    apply_run(
        &fx,
        evaluate_command(
            "2",
            goal_evaluation_request(AgentRevisionNumber::INITIAL.next()),
        ),
    )
    .await
    .expect("the re-evaluation commits");
    answer_evaluation(&fx).await;
    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Satisfied);
}

#[tokio::test]
async fn a_failed_evaluation_leaves_the_run_and_goal_live() {
    // No executor is wired: the dispatch fails closed with the stable code,
    // and — unlike any other effect failure — the coordinator run does not
    // wind down, because the goal must stay decidable.
    let fx = fixture_with(None);
    create_goal_task(&fx, goal_spec_with_evaluator()).await;

    apply_run(
        &fx,
        evaluate_command("1", goal_evaluation_request(AgentRevisionNumber::INITIAL)),
    )
    .await
    .expect("the evaluation commits");
    answer_evaluation(&fx).await;

    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Active);
    assert!(
        !run_status(&fx).await.is_terminal(),
        "the coordinator run stays live"
    );
    let loop_state = run_loop_state(&fx).await;
    assert!(
        loop_state.goal_evaluation().is_none(),
        "no record was produced"
    );
    let failed = loop_state
        .effects()
        .iter()
        .find(|effect| effect.kind() == AgentRunEffectKind::GoalEvaluationCall)
        .expect("the effect is held");
    assert_eq!(
        failed.last_error_code.as_deref(),
        Some("evaluation-executor-missing")
    );

    // The caller re-issues under a fresh operation id once an executor
    // exists; the failed generation does not block it.
    let second = apply_run(
        &fx,
        evaluate_command("2", goal_evaluation_request(AgentRevisionNumber::INITIAL)),
    )
    .await
    .expect("a re-issued evaluation commits");
    assert!(matches!(second, AgentRunEntityReply::Applied { .. }));
}

#[tokio::test]
async fn a_second_evaluation_is_refused_while_one_is_open() {
    let fx = satisfying_fixture();
    create_goal_task(&fx, goal_spec_with_evaluator()).await;

    apply_run(
        &fx,
        evaluate_command("1", goal_evaluation_request(AgentRevisionNumber::INITIAL)),
    )
    .await
    .expect("the first evaluation commits");
    let second = apply_run(
        &fx,
        evaluate_command("2", goal_evaluation_request(AgentRevisionNumber::INITIAL)),
    )
    .await;
    assert_eq!(
        second
            .expect_err("a second in-flight evaluation is refused")
            .code(),
        "run-goal-evaluation-outstanding"
    );
}

#[tokio::test]
async fn a_verification_workflow_evaluation_is_refused_until_it_can_execute() {
    let fx = satisfying_fixture();
    create_goal_task(&fx, goal_spec_with_evaluator()).await;

    let mut request = goal_evaluation_request(AgentRevisionNumber::INITIAL);
    request.method = AgentGoalEvaluationMethod::VerificationWorkflow {
        workflow: rakka_agent::AgentWorkflowToolId::new("verify-ticket")
            .expect("the workflow id is valid"),
    };
    let refused = apply_run(&fx, evaluate_command("1", request)).await;
    assert_eq!(
        refused.expect_err("the workflow method is deferred").code(),
        "run-goal-evaluation-workflow-deferred"
    );
}

#[tokio::test]
async fn an_unbound_goal_refuses_the_evaluation() {
    // A run serving a goal-less task holds nothing to evaluate.
    let fx = satisfying_fixture();
    fx.instantiate_agent().await;
    fx.create_task().await;

    let refused = apply_run(
        &fx,
        evaluate_command("1", goal_evaluation_request(AgentRevisionNumber::INITIAL)),
    )
    .await;
    assert_eq!(
        refused.expect_err("an unbound run refuses").code(),
        "run-goal-evaluation-unbound"
    );
}

#[tokio::test]
async fn a_human_review_approval_satisfies_with_the_resolver_on_record() {
    let fx = satisfying_fixture();
    create_goal_task(&fx, goal_spec_with_evaluator()).await;

    let mut request = goal_evaluation_request(AgentRevisionNumber::INITIAL);
    request.method = AgentGoalEvaluationMethod::HumanReview;
    apply_run(&fx, evaluate_command("1", request))
        .await
        .expect("the human review commits");

    // The effect parks behind its approval checkpoint: no decision, no
    // dispatch, until an authorized human resolves it.
    let effect = evaluation_effect(&fx).await;
    assert!(effect.checkpoint_required, "the review is checkpoint-gated");
    let loop_state = run_loop_state(&fx).await;
    let checkpoint = loop_state
        .open_checkpoints()
        .iter()
        .find(|checkpoint| checkpoint.bound_effect.effect_id == effect.effect_id)
        .expect("the approval checkpoint is open")
        .checkpoint_id
        .clone();

    let resolver = PrincipalRef {
        principal_type: "user".to_string(),
        principal_id: "goal-approver".to_string(),
        display_name: None,
    };
    apply_run(
        &fx,
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                "review-1",
            )
            .expect("the decision key derives"),
            checkpoint_id: checkpoint,
            resolver: resolver.clone(),
            decision: Box::new(rakka_agent::AgentCheckpointDecision::Approval(
                rakka_agent::AgentApprovalDecision::Approve {
                    credential_binding: None,
                    expires_at: AgentTimestampMillis::new(1_000_000),
                    allowed_use_count: 1,
                },
            )),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
    )
    .await
    .expect("the approval applies");

    // The grant is the verdict: the answer builds the record from it, and
    // the door satisfies the goal with the resolver on the durable decision.
    answer_evaluation(&fx).await;
    let goal = goal_view(&fx).await;
    assert_eq!(goal.status, AgentGoalStatus::Satisfied);
    let loop_state = run_loop_state(&fx).await;
    let cell = loop_state
        .goal_evaluation()
        .expect("the cell holds the record");
    assert_eq!(cell.record.evaluated_by.as_ref(), Some(&resolver));
    assert_eq!(
        cell.record.method,
        AgentGoalEvaluationMethodKind::HumanReview
    );
    assert!(
        cell.record
            .evidence
            .iter()
            .any(|item| item.class == "human-decision"),
        "the authorized decision is the evidence"
    );
}

#[tokio::test]
async fn a_denied_human_review_fails_the_evaluation_and_the_goal_stays_active() {
    let fx = satisfying_fixture();
    create_goal_task(&fx, goal_spec_with_evaluator()).await;

    let mut request = goal_evaluation_request(AgentRevisionNumber::INITIAL);
    request.method = AgentGoalEvaluationMethod::HumanReview;
    apply_run(&fx, evaluate_command("1", request))
        .await
        .expect("the human review commits");
    let effect = evaluation_effect(&fx).await;
    let loop_state = run_loop_state(&fx).await;
    let checkpoint = loop_state
        .open_checkpoints()
        .iter()
        .find(|checkpoint| checkpoint.bound_effect.effect_id == effect.effect_id)
        .expect("the approval checkpoint is open")
        .checkpoint_id
        .clone();

    apply_run(
        &fx,
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                "review-1",
            )
            .expect("the decision key derives"),
            checkpoint_id: checkpoint,
            resolver: PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "goal-approver".to_string(),
                display_name: None,
            },
            decision: Box::new(rakka_agent::AgentCheckpointDecision::Approval(
                rakka_agent::AgentApprovalDecision::Deny {
                    reason: "not convinced".to_string(),
                },
            )),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
    )
    .await
    .expect("the denial applies");

    // A denial is a failed evaluation, never a negative criteria decision:
    // the goal stays active and can be re-evaluated, and the coordinator run
    // outlives it.
    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Active);
    assert!(
        !run_status(&fx).await.is_terminal(),
        "the coordinator run stays live"
    );
    assert!(
        run_loop_state(&fx).await.goal_evaluation().is_none(),
        "no record was produced"
    );
}

#[test]
fn the_evaluation_spec_defaults_read_only_with_two_attempts() {
    // An evaluation judges evidence and never mutates the world: read-only is
    // what makes a crash-retry safe, with a small retry budget for transient
    // executor failures.
    let policies = rakka_agent::AgentEffectPolicies::new();
    let request = AgentRunEffectRequest::Evaluation {
        evaluation: Box::new(goal_evaluation_request(AgentRevisionNumber::INITIAL)),
    };
    let spec = policies.spec_for(&request);
    assert_eq!(
        spec.safety_class,
        rakka_agent::AgentEffectSafetyClass::ReadOnly
    );
    assert_eq!(
        spec.max_attempts,
        rakka_agent::AGENT_GOAL_EVALUATION_DEFAULT_MAX_ATTEMPTS
    );
    assert!(!spec.checkpoint_required, "human review opts in at commit");
}

#[tokio::test]
async fn without_a_configured_evaluator_the_commanded_decision_still_stands() {
    // The 4.1 contract is preserved: `evaluator: None` means the
    // application's command authority decides, still revision-fenced.
    let fx = fixture_with(None);
    create_goal_task(&fx, goal_spec()).await;

    let applied = fx
        .apply_task_command(AgentTaskEntityCommand::RecordGoalDecision {
            operation_id: operation("decide"),
            decision: Box::new(AgentGoalDecision {
                reason: AgentGoalTerminalReason::CriteriaSatisfied,
                evaluation: Some(Box::new(goal_evaluation())),
                provenance: Some(Box::new(provenance(70))),
                expected_status_revision: AgentRevisionNumber::INITIAL,
            }),
        })
        .await
        .expect("the commanded decision applies");
    assert!(matches!(applied, AgentTaskEntityReply::Applied { .. }));
    assert_eq!(goal_view(&fx).await.status, AgentGoalStatus::Satisfied);

    // The run counter never moved: the whole flow was command-side.
    assert_eq!(fx.dispatcher.tool_calls(), 0);
    let _ = Ordering::SeqCst;
}
