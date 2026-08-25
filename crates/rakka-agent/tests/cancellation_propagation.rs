//! Durable cancellation propagation across the delegation tree
//! ([specification 8.7](../../docs/plans/rakka-agent/spec.md), scenarios 29
//! and 31).
//!
//! A root cancellation is a request with an observable outcome at every leg:
//! the task records a nonterminal marker and owes the run-cancel exchange to
//! its accepted run, the winding-down run owes one delegation-cancel per
//! created, unsettled child, the child records its own marker and recurses —
//! and nobody projects terminal `Cancelled` while a started consequential
//! effect's outcome is unknown. Every entity here is rebuilt from durable
//! state per call — the `Fixture` convention — so every leg already survives
//! a restart, and the send executor's invocation log proves no re-driven
//! propagation ever replays a child's opaque effect.

mod common;

use std::sync::atomic::Ordering;

use common::{
    child_result_envelope, committed_children, create_fan_out_task, create_real_child,
    delegation_config_with_fan_in, delegation_tool_id, fan_in_tool_id, fan_out_fixture,
    goal_spec_draft, goal_spec_with_fan_out, goal_task_creation_command, proposing_turn, run_scope,
    task_definition, task_scope, Fixture, SkillNamedExecutor, SKILL, SKILL_2, TENANT,
};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    delegation_cancel_operation_id, run_cancel_operation_id, AgentCancellationProgress,
    AgentDelegationCancelOutcome, AgentDelegationCancelRequest, AgentEntityAddress,
    AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload, AgentFanInPolicy,
    AgentLoopPhase, AgentModelTurn, AgentRunCancelRequest, AgentRunEntityCommand, AgentRunStatus,
    AgentTaskId, AgentTaskScope, AgentTaskStatus, AgentToolCallId, AgentToolCallRequest, TenantId,
    AGENT_DELEGATION_CANCEL_PAYLOAD_TYPE, AGENT_RUN_CANCEL_PAYLOAD_TYPE,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentCorrelationId;
use serde_json::json;

/// Drives the root task, the coordinator run, and every named child scope
/// until nothing moves: the multi-entity pump propagation needs.
async fn pump_tree(fixture: &Fixture, children: &[AgentTaskScope]) {
    for _round in 0..64 {
        fixture
            .settle_task_at(&task_scope())
            .await
            .expect("the root settles");
        for scope in children {
            fixture
                .settle_task_at(scope)
                .await
                .expect("the child settles");
        }
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        let progress = run
            .settle_side_effects(&fixture.router, fixture.now())
            .await
            .expect("the run settles");
        let answered = fixture
            .dispatcher
            .drive(&mut run, &fixture.router, fixture.now())
            .await
            .expect("the dispatcher drives");
        if answered == 0
            && progress.transitions == 0
            && progress.effects_dispatched == 0
            && progress.settled == 0
            && progress.failed == 0
            && progress.outstanding == 0
        {
            return;
        }
    }
    panic!("the tree pump did not quiesce");
}

/// Delivers one exchange to a task entity at `scope` and returns its refusal
/// code, when it was refused.
async fn deliver_to_task(
    fixture: &Fixture,
    scope: &AgentTaskScope,
    envelope: &AgentExchangeEnvelope,
) -> Option<String> {
    let mut task = rakka_agent::AgentTaskEntityStore::new(
        scope.clone(),
        fixture.tasks.clone(),
        fixture.agents.clone(),
        fixture.history.clone(),
    );
    let reply = task
        .accept(envelope, &fixture.router, fixture.now())
        .await
        .expect("the delivery succeeds");
    reply
        .result()
        .status()
        .rejection_code()
        .map(ToString::to_string)
}

async fn run_view(fixture: &Fixture) -> (Option<AgentRunStatus>, AgentCancellationProgress) {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let progress = state
        .run()
        .map_or(AgentCancellationProgress::NotRequested, |run| {
            AgentCancellationProgress::derive(run)
        });
    (state.status(), progress)
}

async fn root_snapshot(fixture: &Fixture) -> rakka_agent::AgentTaskSnapshot {
    fixture.task_snapshot().await
}

/// Scenario 31's in-fabric spine, end to end over real child task entities:
/// the root cancel defers the root task, winds the coordinator down through
/// the run-cancel exchange, chases both created children with
/// delegation-cancel exchanges the children durably accept, recurses into
/// their own immediate finalization, and completes only after both terminal
/// reports return — with the send executor's log proving no re-driven leg
/// replayed a child's opaque send (scenario 29's at-most-once half).
#[tokio::test]
async fn a_root_cancel_propagates_to_real_children_and_finalizes_on_their_reports() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the fan-out parks");
    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 2);
    assert_eq!(executor.invocations(), 2, "one send per child");

    for (index, (delegation, child_task)) in children.iter().enumerate() {
        create_real_child(
            &fixture,
            delegation,
            child_task,
            if index == 0 { SKILL } else { SKILL_2 },
        )
        .await;
    }
    let child_scopes: Vec<AgentTaskScope> = children
        .iter()
        .map(|(_, child)| {
            AgentTaskScope::new(TenantId::new(TENANT), child.clone()).expect("the scope is valid")
        })
        .collect();

    // The root cancel: the goal decides at request time, the task stays
    // nonterminal, and the run-cancel exchange is owed — not yet delivered.
    let reply = fixture
        .apply_task_command(rakka_agent::AgentTaskEntityCommand::Cancel {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, task_scope().task().as_str(), "root-cancel"],
            )
            .expect("the operation id derives"),
            reason: "operator".to_string(),
        })
        .await
        .expect("the cancel applies");
    let outcome = match reply {
        rakka_agent::AgentTaskEntityReply::Applied { outcome } => outcome,
        other => panic!("expected the cancel to apply, got {other:?}"),
    };
    assert_eq!(outcome.status, AgentTaskStatus::InProgress);
    let view = root_snapshot(&fixture).await;
    assert!(view.cancellation.is_some(), "the request marker is durable");
    assert_eq!(
        AgentCancellationProgress::derive_task(&view),
        AgentCancellationProgress::Propagating
    );

    // Propagation drives the whole tree to rest.
    pump_tree(&fixture, &child_scopes).await;
    pump_tree(&fixture, &child_scopes).await;

    // Every child durably recorded the request, finalized immediately — no
    // assignment was ever live — and reported terminal `Cancelled` upward.
    for scope in &child_scopes {
        let state = rakka_agent::load_agent_task_state(
            &fixture.tasks,
            scope,
            &rakka_agent::AgentSchemaPolicy::default(),
        )
        .await
        .expect("the child state loads")
        .expect("the child exists");
        let snapshot = state.snapshot().expect("the child snapshot derives");
        assert_eq!(snapshot.status, AgentTaskStatus::Cancelled);
        assert_eq!(
            snapshot
                .terminal_reason
                .as_ref()
                .map(|reason| reason.code()),
            Some("cancellation-requested")
        );
    }

    // The parent quiesced on the last report and terminalized under the
    // reason the wind-down began with; its cells hold both the accepted
    // cancel receipts and the terminal child results.
    let (status, progress) = run_view(&fixture).await;
    assert_eq!(status, Some(AgentRunStatus::Cancelled));
    assert_eq!(progress, AgentCancellationProgress::Completed);
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("loop state");
    for (delegation, _) in &children {
        let cell = loop_state.delegation(delegation).expect("the cell exists");
        assert!(
            matches!(
                cell.cancel,
                Some(AgentDelegationCancelOutcome::Accepted { .. })
            ),
            "the chase settled accepted"
        );
        let result = cell.result.clone().expect("the child's report recorded");
        assert_eq!(result.status, AgentTaskStatus::Cancelled);
    }

    // The ledger closed, so the root finalized under the marker's reason.
    let view = root_snapshot(&fixture).await;
    assert_eq!(view.status, AgentTaskStatus::Cancelled);
    assert_eq!(
        view.terminal_reason.as_ref().map(|reason| reason.code()),
        Some("cancellation-requested")
    );
    assert_eq!(
        AgentCancellationProgress::derive_task(&view),
        AgentCancellationProgress::Completed
    );

    // Scenario 29's at-most-once half: every restart-shaped re-drive above
    // converged on the journal and the cells; no leg re-invoked a send.
    assert_eq!(
        executor.invocations(),
        2,
        "no propagation leg replays a send"
    );
}

/// The receiving fences answer definitively on forged or misrouted requests,
/// and the request legs authenticate exactly against durable identity.
#[tokio::test]
async fn cancel_requests_authenticate_against_durable_identity() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the fan-out parks");

    let tenant = TenantId::new(TENANT);
    let generation = {
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        run.state()
            .expect("state")
            .run()
            .expect("the run accepted")
            .generation
    };

    // A run-cancel from a task the run does not serve is forged.
    let forged_scope = AgentTaskScope::new(tenant.clone(), AgentTaskId::new("intruder").unwrap())
        .expect("the scope is valid");
    let operation = run_cancel_operation_id(&forged_scope, generation).expect("derives");
    let payload = AgentExchangePayload::encode(
        AGENT_RUN_CANCEL_PAYLOAD_TYPE,
        &AgentRunCancelRequest {
            task: forged_scope.clone(),
            generation,
            reason: "forged".to_string(),
        },
    )
    .expect("encodes");
    let envelope = AgentExchangeEnvelope::new(
        operation.clone(),
        AgentExchangeKind::RunCancel,
        AgentEntityAddress::Task(forged_scope),
        AgentEntityAddress::Run(run_scope()),
        payload,
        AgentCorrelationId::new(operation.as_str()),
        fixture.now(),
    )
    .expect("the envelope is valid");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let reply = run
        .accept(&envelope, &fixture.router, fixture.now())
        .await
        .expect("the delivery succeeds");
    assert_eq!(
        reply.result().status().rejection_code(),
        Some("run-cancel-forged")
    );

    // A stale generation refuses without winding anything down.
    let operation =
        run_cancel_operation_id(&task_scope(), generation.next()).expect("the operation derives");
    let payload = AgentExchangePayload::encode(
        AGENT_RUN_CANCEL_PAYLOAD_TYPE,
        &AgentRunCancelRequest {
            task: task_scope(),
            generation: generation.next(),
            reason: "stale".to_string(),
        },
    )
    .expect("encodes");
    let envelope = AgentExchangeEnvelope::new(
        operation.clone(),
        AgentExchangeKind::RunCancel,
        AgentEntityAddress::Task(task_scope()),
        AgentEntityAddress::Run(run_scope()),
        payload,
        AgentCorrelationId::new(operation.as_str()),
        fixture.now(),
    )
    .expect("the envelope is valid");
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let reply = run
        .accept(&envelope, &fixture.router, fixture.now())
        .await
        .expect("the delivery succeeds");
    assert_eq!(
        reply.result().status().rejection_code(),
        Some("run-cancel-stale-generation")
    );
    let (status, _) = run_view(&fixture).await;
    assert_eq!(status, Some(AgentRunStatus::Running), "nothing wound down");

    // A delegation-cancel against a task with no delegation provenance is
    // undeliverable definitively: the root task was never delegated.
    let delegation = committed_children(&fixture).await[0].0.clone();
    let operation = delegation_cancel_operation_id(&tenant, &delegation).expect("derives");
    let payload = AgentExchangePayload::encode(
        AGENT_DELEGATION_CANCEL_PAYLOAD_TYPE,
        &AgentDelegationCancelRequest {
            delegation,
            child_task: task_scope().task().clone(),
            reason: "misrouted".to_string(),
        },
    )
    .expect("encodes");
    let envelope = AgentExchangeEnvelope::new(
        operation.clone(),
        AgentExchangeKind::DelegationCancel,
        AgentEntityAddress::Run(run_scope()),
        AgentEntityAddress::Task(task_scope()),
        payload,
        AgentCorrelationId::new(operation.as_str()),
        fixture.now(),
    )
    .expect("the envelope is valid");
    let code = deliver_to_task(&fixture, &task_scope(), &envelope).await;
    assert_eq!(code.as_deref(), Some("delegation-cancel-not-delegated"));
}

/// A goal whose deadline passed is expired by the settle pass, and the same
/// pass converts the terminal decision into the root task's cancellation
/// request — no command arrives anywhere.
#[tokio::test]
async fn a_goal_deadline_expiry_propagates_from_the_settle_pass() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    fixture.instantiate_agent().await;
    let mut spec = goal_spec_with_fan_out(None, None);
    spec.deadline = Some(rakka_agent_workflow::AgentTimestampMillis::new(
        fixture.now().as_millis() + 50,
    ));
    fixture
        .apply_task_command(goal_task_creation_command(
            task_definition(),
            goal_spec_draft(spec, true),
        ))
        .await
        .expect("the goal task should create");
    fixture.pump().await.expect("the fan-out parks");

    let due = fixture.now().as_millis() + 60_000;
    fixture.clock.store(due, Ordering::SeqCst);
    fixture
        .settle_task_at(&task_scope())
        .await
        .expect("the settle pass runs");

    let view = root_snapshot(&fixture).await;
    assert!(!view.status.is_terminal(), "the task defers to its run");
    let marker = view.cancellation.as_ref().expect("the marker is set");
    assert!(
        marker.detail().starts_with("goal-"),
        "the reason names the goal decision: {}",
        marker.detail()
    );
    assert_eq!(
        view.goal_state
            .expect("the goal view exists")
            .terminal
            .map(|reason| reason.code()),
        Some("deadline-expired")
    );

    // The run-cancel the pass owed winds the coordinator down.
    fixture.pump().await.expect("propagation drives");
    let (status, _) = run_view(&fixture).await;
    assert!(
        matches!(
            status,
            Some(AgentRunStatus::Cancelling | AgentRunStatus::Cancelled)
        ),
        "the coordinator wound down, got {status:?}"
    );
}

/// An `Any` group satisfied by its first child chases the straggler it left
/// behind: the resolution itself is the chase condition, and the straggler's
/// cell records the settled outcome of its delegation-cancel exchange.
#[tokio::test]
async fn an_any_resolution_chases_the_straggler() {
    let executor = SkillNamedExecutor::new();
    let fixture = fan_out_fixture(executor.clone());
    create_fan_out_task(&fixture, Some(AgentFanInPolicy::Any)).await;
    fixture.pump().await.expect("the fan-out parks");
    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 2);
    for (index, (delegation, child_task)) in children.iter().enumerate() {
        create_real_child(
            &fixture,
            delegation,
            child_task,
            if index == 0 { SKILL } else { SKILL_2 },
        )
        .await;
    }
    let child_scopes: Vec<AgentTaskScope> = children
        .iter()
        .map(|(_, child)| {
            AgentTaskScope::new(TenantId::new(TENANT), child.clone()).expect("the scope is valid")
        })
        .collect();

    // The first child reports success; `Any` resolves on it. The report is
    // synthetic — human-task completion is a later slice — but it carries the
    // real child's identity, so every parent-side fence passes.
    let (first_delegation, first_child) = &children[0];
    let envelope = child_result_envelope(
        &fixture,
        first_delegation,
        first_child,
        AgentTaskStatus::Completed,
    );
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let reply = run
        .accept(&envelope, &fixture.router, fixture.now())
        .await
        .expect("the delivery succeeds");
    assert!(reply.result().is_accepted());
    pump_tree(&fixture, &child_scopes).await;
    pump_tree(&fixture, &child_scopes).await;

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("loop state");
    let resolution = loop_state
        .fan_in()
        .expect("the group is retained")
        .resolution
        .clone()
        .expect("the group resolved");
    assert!(resolution.satisfied, "any satisfied by the first child");
    assert!(
        loop_state
            .delegation(first_delegation)
            .expect("the cell exists")
            .cancel
            .is_none(),
        "the satisfying child is never chased"
    );
    let (straggler_delegation, _) = &children[1];
    let straggler = loop_state
        .delegation(straggler_delegation)
        .expect("the cell exists");
    assert!(
        straggler.cancel.is_some(),
        "the resolution chased the straggler"
    );
    // The chased child durably recorded the request and cancelled itself.
    let straggler_state = rakka_agent::load_agent_task_state(
        &fixture.tasks,
        &child_scopes[1],
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the child state loads")
    .expect("the child exists");
    let snapshot = straggler_state.snapshot().expect("derives");
    assert_eq!(snapshot.status, AgentTaskStatus::Cancelled);
    // The satisfied coordinator itself keeps going: a chase never winds the
    // parent down, and the resumed model proposes the parent's own result.
    assert_eq!(
        state.status(),
        Some(AgentRunStatus::Completed),
        "the coordinator survived the chase and completed"
    );
    let phase = loop_state.phase();
    assert_ne!(phase, AgentLoopPhase::Suspended);
}

/// A fired fan-in deadline chases the very stragglers it timed out.
///
/// The deadline marks its unresolved members `timed_out` *before* resolving
/// the group, so the chase cannot read them as "unresolved" — it reads the
/// members the group never got a terminal report from. Without that the
/// deadline branch would chase nobody: the group resolves, the parent moves
/// on, and the child it stopped waiting for keeps running and spending with
/// no cancellation request ever sent.
#[tokio::test]
async fn a_fired_deadline_chases_the_stragglers_it_timed_out() {
    let executor = SkillNamedExecutor::new();
    let fixture = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(
                    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                        .with_text("Fanning out with a wait deadline.")
                        .with_tool_call(
                            AgentToolCallRequest::new(
                                AgentToolCallId::new("delegate-1").expect("call id"),
                                delegation_tool_id(),
                                json!({ "skill": SKILL, "input": { "text": "hello" } }),
                            )
                            .expect("the tool call is bounded"),
                        )
                        .with_tool_call(
                            AgentToolCallRequest::new(
                                AgentToolCallId::new("await-1").expect("call id"),
                                fan_in_tool_id(),
                                json!({ "deadline": 5_000 }),
                            )
                            .expect("the tool call is bounded"),
                        ),
                )
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(executor.clone()),
    )
    .with_delegation(delegation_config_with_fan_in());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the fan-out parks");
    let children = committed_children(&fixture).await;
    assert_eq!(children.len(), 1);
    let (delegation, child_task) = children[0].clone();
    create_real_child(&fixture, &delegation, &child_task, SKILL).await;
    let child_scope =
        AgentTaskScope::new(TenantId::new(TENANT), child_task.clone()).expect("the scope is valid");

    // Past due: the straggler is marked timed out and the group resolves.
    fixture.clock.store(10_000, Ordering::SeqCst);
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.apply(
        AgentRunEntityCommand::FireFanInDeadline {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::Command,
                [TENANT, "fan-in-deadline", "due"],
            )
            .expect("the operation id derives"),
        },
        &fixture.router,
        fixture.now(),
    )
    .await
    .expect("the due fire applies");

    pump_tree(&fixture, std::slice::from_ref(&child_scope)).await;
    pump_tree(&fixture, std::slice::from_ref(&child_scope)).await;

    // The timed-out member was chased: its cell records the settled outcome
    // of a delegation-cancel, and the real child durably cancelled itself.
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("loop state");
    let cell = loop_state.delegation(&delegation).expect("the cell exists");
    assert!(
        cell.cancel.is_some(),
        "the fired deadline chased the member it timed out"
    );
    let child = rakka_agent::load_agent_task_state(
        &fixture.tasks,
        &child_scope,
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the child state loads")
    .expect("the child exists");
    assert_eq!(
        child.snapshot().expect("the snapshot derives").status,
        AgentTaskStatus::Cancelled
    );
    // The chase never winds the parent down: it resolved on the timeout table
    // and went on to propose its own result.
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
}
