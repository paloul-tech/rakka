//! Durable step runner state-machine tests.

use std::collections::BTreeMap;

use rakka_agent_workflow::{
    agent_run_persistence_id, AgentPayloadDescriptor, AgentRunEngineError, AgentRunId,
    AgentRunState, AgentRunStatus, AgentRunTransitionKind, AgentRunWaitReason, AgentStatePayload,
    AgentStep, AgentStepId, AgentStepKind, AgentStepRunner, AgentStepSuccess, AgentTenantId,
    AgentTimestampMillis, AgentWorkflow, AgentWorkflowId, HumanCheckpointId, InlineState,
    StateSchemaVersion, WorkflowDefinitionVersion,
};
use rakka_persistence::{DurableStateStore, InMemoryDurableStateStore, Revision};

type TestStore = InMemoryDurableStateStore<AgentRunState>;

#[tokio::test]
async fn start_persists_run_state_and_recovered_runner_continues() {
    let workflow = two_step_workflow();
    let store = TestStore::new();
    let run_id = AgentRunId::new("run-start-recover");
    let mut runner = AgentStepRunner::new(workflow.clone(), run_id.clone(), store.clone());

    assert_error_code(
        runner
            .state()
            .expect_err("state before recovery should fail"),
        "not-recovered",
    );
    assert!(runner
        .recover()
        .await
        .expect("initial recovery should succeed")
        .is_none());

    let started = runner
        .start(accepted_run_state(
            &workflow,
            &run_id,
            AgentStepId::new("plan"),
        ))
        .await
        .expect("start should persist accepted run state");
    assert_eq!(started.kind, AgentRunTransitionKind::Start);
    assert_eq!(started.previous_status, None);
    assert_eq!(started.next_status, AgentRunStatus::Accepted);
    assert_eq!(started.revision, Revision::new(1));
    assert_eq!(store.len(), 1);

    let stored = store
        .load(&agent_run_persistence_id(&run_id))
        .await
        .expect("durable load should succeed")
        .expect("started run should be persisted");
    assert_eq!(stored.state.status, AgentRunStatus::Accepted);

    let mut recovered = AgentStepRunner::new(workflow, run_id, store);
    let recovered_state = recovered
        .recover()
        .await
        .expect("fresh runner should recover")
        .expect("fresh runner should see stored state");
    assert_eq!(recovered_state.status, AgentRunStatus::Accepted);

    let began = recovered
        .begin_step(ts(200))
        .await
        .expect("recovered accepted run should begin");
    assert_eq!(began.kind, AgentRunTransitionKind::BeginStep);
    assert_eq!(began.previous_status, Some(AgentRunStatus::Accepted));
    assert_eq!(began.next_status, AgentRunStatus::Running);
    assert_eq!(began.revision, Revision::new(2));
    assert_eq!(began.state.current_attempt, 1);
    assert_eq!(began.state.updated_at, ts(200));
}

#[tokio::test]
async fn step_success_advances_to_next_step() {
    let workflow = two_step_workflow();
    let run_id = AgentRunId::new("run-advance");
    let mut runner = start_and_begin(workflow, run_id, TestStore::new()).await;

    let advanced = runner
        .succeed_step(
            AgentStepSuccess::advance(AgentStepId::new("review"), inline_payload("ready")),
            ts(300),
        )
        .await
        .expect("running step should advance");

    assert_eq!(advanced.kind, AgentRunTransitionKind::StepSucceeded);
    assert_eq!(advanced.previous_status, Some(AgentRunStatus::Running));
    assert_eq!(advanced.next_status, AgentRunStatus::Running);
    assert_eq!(
        advanced.state.current_step_id,
        Some(AgentStepId::new("review"))
    );
    assert_eq!(advanced.state.current_attempt, 0);
    assert_eq!(advanced.state.state_payload, inline_payload("ready"));
    assert_eq!(advanced.state.completed_at, None);
}

#[tokio::test]
async fn step_success_without_next_step_completes() {
    let workflow = single_step_workflow();
    let run_id = AgentRunId::new("run-complete-from-step");
    let mut runner = start_and_begin(workflow, run_id, TestStore::new()).await;

    let completed = runner
        .succeed_step(AgentStepSuccess::complete(inline_payload("done")), ts(300))
        .await
        .expect("running step without next step should complete");

    assert_eq!(completed.kind, AgentRunTransitionKind::Complete);
    assert_eq!(completed.previous_status, Some(AgentRunStatus::Running));
    assert_eq!(completed.next_status, AgentRunStatus::Completed);
    assert_eq!(completed.state.current_step_id, None);
    assert_eq!(completed.state.state_payload, inline_payload("done"));
    assert_eq!(completed.state.completed_at, Some(ts(300)));
}

#[tokio::test]
async fn step_failure_and_run_failure_mark_failed() {
    let workflow = single_step_workflow();
    let running_run_id = AgentRunId::new("run-step-failure");
    let mut running = start_and_begin(workflow.clone(), running_run_id, TestStore::new()).await;

    let step_failed = running
        .fail_step("tool-timeout", ts(300))
        .await
        .expect("running step should fail the run");
    assert_eq!(step_failed.kind, AgentRunTransitionKind::StepFailed);
    assert_eq!(step_failed.previous_status, Some(AgentRunStatus::Running));
    assert_eq!(step_failed.next_status, AgentRunStatus::Failed);
    assert_eq!(step_failed.state.completed_at, Some(ts(300)));

    let accepted_run_id = AgentRunId::new("run-direct-failure");
    let mut accepted = start_only(
        workflow,
        accepted_run_id,
        AgentStepId::new("plan"),
        TestStore::new(),
    )
    .await;
    let run_failed = accepted
        .fail_run("policy-rejected", ts(250))
        .await
        .expect("non-terminal run should fail");
    assert_eq!(run_failed.kind, AgentRunTransitionKind::Fail);
    assert_eq!(run_failed.previous_status, Some(AgentRunStatus::Accepted));
    assert_eq!(run_failed.next_status, AgentRunStatus::Failed);
    assert_eq!(run_failed.state.completed_at, Some(ts(250)));
}

#[tokio::test]
async fn wait_and_resume_cover_all_wait_statuses() {
    let workflow = single_step_workflow();
    let run_id = AgentRunId::new("run-waits");
    let mut runner = start_and_begin(workflow, run_id, TestStore::new()).await;

    let timer_wait = runner
        .wait(AgentRunWaitReason::Timer, ts(300))
        .await
        .expect("running run should wait for timer");
    assert_eq!(timer_wait.kind, AgentRunTransitionKind::WaitForTimer);
    assert_eq!(timer_wait.next_status, AgentRunStatus::WaitingForTimer);

    let timer_resumed = runner
        .resume(ts(310))
        .await
        .expect("timer wait should resume");
    assert_eq!(timer_resumed.kind, AgentRunTransitionKind::Resume);
    assert_eq!(
        timer_resumed.previous_status,
        Some(AgentRunStatus::WaitingForTimer)
    );
    assert_eq!(timer_resumed.next_status, AgentRunStatus::Running);

    let checkpoint_id = HumanCheckpointId::new("checkpoint-review");
    let human_wait = runner
        .wait(
            AgentRunWaitReason::Human {
                checkpoint_id: checkpoint_id.clone(),
            },
            ts(320),
        )
        .await
        .expect("running run should wait for human decision");
    assert_eq!(human_wait.kind, AgentRunTransitionKind::WaitForHuman);
    assert_eq!(human_wait.next_status, AgentRunStatus::WaitingForHuman);
    assert_eq!(
        human_wait.state.pending_human_checkpoint.as_ref(),
        Some(&checkpoint_id)
    );

    let human_resumed = runner
        .resume(ts(330))
        .await
        .expect("human wait should resume");
    assert_eq!(human_resumed.next_status, AgentRunStatus::Running);
    assert_eq!(human_resumed.state.pending_human_checkpoint, None);

    let effect_wait = runner
        .wait(AgentRunWaitReason::Effect, ts(340))
        .await
        .expect("running run should wait for effect");
    assert_eq!(effect_wait.kind, AgentRunTransitionKind::WaitForEffect);
    assert_eq!(effect_wait.next_status, AgentRunStatus::WaitingForEffect);

    let effect_resumed = runner
        .resume(ts(350))
        .await
        .expect("effect wait should resume");
    assert_eq!(
        effect_resumed.previous_status,
        Some(AgentRunStatus::WaitingForEffect)
    );
    assert_eq!(effect_resumed.next_status, AgentRunStatus::Running);
}

#[tokio::test]
async fn cancellation_and_compensation_statuses_are_supported() {
    let workflow = single_step_workflow();
    let cancelling_run_id = AgentRunId::new("run-cancelling");
    let mut cancelling = start_only(
        workflow.clone(),
        cancelling_run_id,
        AgentStepId::new("plan"),
        TestStore::new(),
    )
    .await;

    let requested = cancelling
        .request_cancellation(
            "user-requested",
            Some("operator requested cancellation".to_string()),
            ts(250),
        )
        .await
        .expect("accepted run should enter cancelling");
    assert_eq!(requested.kind, AgentRunTransitionKind::RequestCancellation);
    assert_eq!(requested.next_status, AgentRunStatus::Cancelling);
    assert_eq!(
        requested
            .state
            .cancellation
            .as_ref()
            .expect("cancellation details should be stored")
            .reason_code,
        "user-requested"
    );

    let cancelled = cancelling
        .cancel(ts(260))
        .await
        .expect("cancelling run should become cancelled");
    assert_eq!(cancelled.kind, AgentRunTransitionKind::Cancel);
    assert_eq!(cancelled.next_status, AgentRunStatus::Cancelled);
    assert_eq!(cancelled.state.completed_at, Some(ts(260)));

    let failed_run_id = AgentRunId::new("run-compensating");
    let mut failed = start_and_begin(workflow, failed_run_id, TestStore::new()).await;
    failed
        .fail_step("model-error", ts(300))
        .await
        .expect("running run should fail before compensation");
    let compensating = failed
        .begin_compensation(ts(325))
        .await
        .expect("failed run should begin compensation");
    assert_eq!(compensating.kind, AgentRunTransitionKind::BeginCompensation);
    assert_eq!(compensating.previous_status, Some(AgentRunStatus::Failed));
    assert_eq!(compensating.next_status, AgentRunStatus::Compensating);
    assert_eq!(compensating.state.completed_at, None);
}

#[tokio::test]
async fn invalid_transitions_fail_with_stable_error_codes() {
    let workflow = single_step_workflow();
    let store = TestStore::new();
    let run_id = AgentRunId::new("run-invalid");
    let mut runner = AgentStepRunner::new(workflow.clone(), run_id.clone(), store.clone());

    assert_error_code(
        runner
            .begin_step(ts(200))
            .await
            .expect_err("begin before recovery should fail"),
        "not-recovered",
    );

    runner
        .recover()
        .await
        .expect("initial recovery should succeed");
    assert_error_code(
        runner
            .begin_step(ts(200))
            .await
            .expect_err("begin before start should fail"),
        "missing-run-state",
    );

    runner
        .start(accepted_run_state(
            &workflow,
            &run_id,
            AgentStepId::new("plan"),
        ))
        .await
        .expect("accepted run should start");
    assert_error_code(
        runner
            .resume(ts(210))
            .await
            .expect_err("resume from accepted should fail"),
        "invalid-transition",
    );
    assert_error_code(
        runner
            .start(accepted_run_state(
                &workflow,
                &run_id,
                AgentStepId::new("plan"),
            ))
            .await
            .expect_err("second start should fail"),
        "run-already-started",
    );

    let unknown_run_id = AgentRunId::new("run-unknown-step");
    let mut unknown = AgentStepRunner::new(workflow.clone(), unknown_run_id.clone(), store);
    unknown
        .recover()
        .await
        .expect("unknown-step runner should recover");
    assert_error_code(
        unknown
            .start(accepted_run_state(
                &workflow,
                &unknown_run_id,
                AgentStepId::new("missing"),
            ))
            .await
            .expect_err("unknown step should fail validation"),
        "unknown-step",
    );
}

async fn start_and_begin(
    workflow: AgentWorkflow,
    run_id: AgentRunId,
    store: TestStore,
) -> AgentStepRunner<TestStore> {
    let mut runner = start_only(workflow, run_id, AgentStepId::new("plan"), store).await;
    runner
        .begin_step(ts(200))
        .await
        .expect("accepted run should begin");
    runner
}

async fn start_only(
    workflow: AgentWorkflow,
    run_id: AgentRunId,
    first_step_id: AgentStepId,
    store: TestStore,
) -> AgentStepRunner<TestStore> {
    let mut runner = AgentStepRunner::new(workflow.clone(), run_id.clone(), store);
    runner
        .recover()
        .await
        .expect("initial recovery should succeed");
    runner
        .start(accepted_run_state(&workflow, &run_id, first_step_id))
        .await
        .expect("accepted run should start");
    runner
}

fn accepted_run_state(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
    first_step_id: AgentStepId,
) -> AgentRunState {
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new("tenant-test")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        status: AgentRunStatus::Accepted,
        current_step_id: Some(first_step_id),
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: ts(100),
        updated_at: ts(100),
        completed_at: None,
    }
}

fn two_step_workflow() -> AgentWorkflow {
    let plan = AgentStepId::new("plan");
    let review = AgentStepId::new("review");
    let mut workflow = base_workflow();
    workflow.steps = vec![
        step(plan, AgentStepKind::Planner, vec![review.clone()]),
        step(review, AgentStepKind::HumanCheckpoint, Vec::new()),
    ];
    workflow
}

fn single_step_workflow() -> AgentWorkflow {
    let mut workflow = base_workflow();
    workflow.steps = vec![step(
        AgentStepId::new("plan"),
        AgentStepKind::Planner,
        Vec::new(),
    )];
    workflow
}

fn base_workflow() -> AgentWorkflow {
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-step-runner"),
        workflow_type: "step-runner".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Step runner workflow".to_string()),
        status_labels: [
            AgentRunStatus::Accepted,
            AgentRunStatus::Running,
            AgentRunStatus::WaitingForTimer,
            AgentRunStatus::WaitingForHuman,
            AgentRunStatus::WaitingForEffect,
            AgentRunStatus::Cancelling,
            AgentRunStatus::Completed,
            AgentRunStatus::Failed,
            AgentRunStatus::Compensating,
            AgentRunStatus::Cancelled,
        ]
        .into_iter()
        .map(|status| status.as_label().to_string())
        .collect(),
        command_types: vec!["StartRun".to_string()],
        steps: Vec::new(),
        payload_types: vec![
            AgentPayloadDescriptor::new("step-runner.input").content_type("application/json")
        ],
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: BTreeMap::from([(
            "workflow_type".to_string(),
            "step-runner".to_string(),
        )]),
    }
}

fn step(step_id: AgentStepId, kind: AgentStepKind, next_step_ids: Vec<AgentStepId>) -> AgentStep {
    AgentStep {
        step_id,
        kind,
        display_name: None,
        next_step_ids,
        timeout_ms: Some(1_000),
        config_ref: None,
        observability_labels: BTreeMap::new(),
    }
}

fn inline_payload(value: &str) -> AgentStatePayload {
    let bytes = value.as_bytes().to_vec();
    AgentStatePayload::Inline(InlineState {
        content_type: "text/plain".to_string(),
        size_bytes: bytes.len() as u64,
        bytes,
    })
}

const fn ts(millis: u64) -> AgentTimestampMillis {
    AgentTimestampMillis::new(millis)
}

fn assert_error_code(error: AgentRunEngineError, expected: &'static str) {
    assert_eq!(error.code(), expected, "{error}");
}
