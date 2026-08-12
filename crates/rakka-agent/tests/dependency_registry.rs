//! The dependents registry
//! ([specification 9.2](../../../docs/plans/rakka-agent/spec.md), the sending
//! half deferred from slice 4.6 to 5.4): a dependent registers its forward
//! edge with the upstream in the same compare-and-set that declares it, the
//! upstream walks its bounded registry at terminalization to owe each
//! dependent its outcome, and every leg is sender-fenced, deduplicated, and
//! answerable past the journal's bounded window.

mod common;

use common::{task_definition, task_scope, tenant, Fixture, TASK, TENANT};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    dependency_outcome_operation_id, dependency_registration_operation_id, load_agent_task_state,
    AgentDependencyFailurePolicy, AgentDependencyOutcomeNotice, AgentDependencyRegistration,
    AgentEntityAddress, AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload,
    AgentOperationId, AgentSchemaPolicy, AgentTaskContent, AgentTaskCreation,
    AgentTaskDependencyDeclaration, AgentTaskDependencyOutcome, AgentTaskEntityCommand,
    AgentTaskEntityStore, AgentTaskId, AgentTaskOwnership, AgentTaskScope, AgentTaskState,
    AgentTaskStatus, AGENT_DEPENDENCY_OUTCOME_PAYLOAD_TYPE,
    AGENT_DEPENDENCY_REGISTRATION_PAYLOAD_TYPE, AGENT_TASK_MAX_DEPENDENTS,
};
use rakka_agent_workflow::AgentCorrelationId;
use serde_json::json;

const UPSTREAM: &str = "human-upstream";

fn upstream_scope() -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(UPSTREAM).expect("the task id is valid"),
    )
    .expect("the scope is valid")
}

fn scope_for(task: &str) -> AgentTaskScope {
    AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(task).expect("the task id is valid"),
    )
    .expect("the scope is valid")
}

fn creation(
    task: &str,
    human: bool,
    dependencies: Vec<AgentTaskDependencyDeclaration>,
) -> AgentTaskEntityCommand {
    let definition = if human {
        task_definition().with_ownership(AgentTaskOwnership::Human)
    } else {
        task_definition()
    };
    AgentTaskEntityCommand::Create {
        operation_id: AgentOperationId::new(
            rakka_agent::AgentOperationKind::TaskCreation,
            [TENANT, task, "1"],
        )
        .expect("the operation id derives"),
        creation: Box::new(AgentTaskCreation {
            definition,
            input: AgentTaskContent::inline(json!({ "ticket": 1 }))
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
    }
}

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
}

async fn state_at(fx: &Fixture, scope: &AgentTaskScope) -> AgentTaskState {
    load_agent_task_state(&fx.tasks, scope, &AgentSchemaPolicy::default())
        .await
        .expect("the state loads")
        .expect("the task exists")
}

/// Delivers one hand-built exchange and returns its refusal code, if any.
async fn deliver_to_task(
    fx: &Fixture,
    scope: &AgentTaskScope,
    envelope: &AgentExchangeEnvelope,
) -> Option<String> {
    let mut task = AgentTaskEntityStore::new(
        scope.clone(),
        fx.tasks.clone(),
        fx.agents.clone(),
        fx.history.clone(),
    );
    let reply = task
        .accept(envelope, &fx.router, fx.now())
        .await
        .expect("the delivery succeeds");
    reply
        .result()
        .status()
        .rejection_code()
        .map(ToString::to_string)
}

fn registration_envelope(
    fx: &Fixture,
    claimed_dependent: &AgentTaskScope,
    initiator: AgentEntityAddress,
    upstream: &AgentTaskId,
) -> AgentExchangeEnvelope {
    let operation_id =
        dependency_registration_operation_id(&tenant(), upstream, claimed_dependent.task())
            .expect("the operation id derives");
    let payload = AgentExchangePayload::encode(
        AGENT_DEPENDENCY_REGISTRATION_PAYLOAD_TYPE,
        &AgentDependencyRegistration {
            dependent: claimed_dependent.clone(),
            upstream: upstream.clone(),
            policy: AgentDependencyFailurePolicy::CancelDependents,
        },
    )
    .expect("the payload encodes");
    AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::DependencyRegistration,
        initiator,
        AgentEntityAddress::Task(scope_for(upstream.as_str())),
        payload,
        AgentCorrelationId::new(operation_id.as_str()),
        fx.now(),
    )
    .expect("the envelope is valid")
}

fn outcome_envelope(
    fx: &Fixture,
    claimed_upstream: &AgentTaskScope,
    initiator: AgentEntityAddress,
    dependent: &AgentTaskScope,
    outcome: AgentTaskDependencyOutcome,
) -> AgentExchangeEnvelope {
    let operation_id =
        dependency_outcome_operation_id(&tenant(), claimed_upstream.task(), dependent.task())
            .expect("the operation id derives");
    let payload = AgentExchangePayload::encode(
        AGENT_DEPENDENCY_OUTCOME_PAYLOAD_TYPE,
        &AgentDependencyOutcomeNotice {
            upstream: claimed_upstream.clone(),
            outcome,
            terminal_reason: Some("result-accepted".to_string()),
            result_digest: None,
        },
    )
    .expect("the payload encodes");
    AgentExchangeEnvelope::new(
        operation_id.clone(),
        AgentExchangeKind::DependencyOutcome,
        initiator,
        AgentEntityAddress::Task(dependent.clone()),
        payload,
        AgentCorrelationId::new(operation_id.as_str()),
        fx.now(),
    )
    .expect("the envelope is valid")
}

/// A registration arriving at an already-terminal upstream is answered with
/// the outcome on the receipt: the dependent unblocks from the receipt
/// alone, no registry entry grows, and no notification is ever owed.
#[tokio::test]
async fn an_already_terminal_upstream_answers_the_registration_with_its_outcome() {
    let fx = fixture();
    fx.apply_task_command_at(&upstream_scope(), creation(UPSTREAM, true, Vec::new()))
        .await
        .expect("the upstream creates");
    // Cancel it terminal before any dependent exists.
    fx.apply_task_command_at(
        &upstream_scope(),
        AgentTaskEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, UPSTREAM, "operator"],
            )
            .expect("the operation id derives"),
            reason: "no longer needed".to_string(),
        },
    )
    .await
    .expect("the cancellation applies");

    // The dependent declares the edge afterwards; its registration settles
    // from the receipt, applying `Cancelled` through the same core the
    // notification would — the dependent cancels under its declared policy.
    fx.apply_task_command_at(
        &task_scope(),
        creation(
            TASK,
            true,
            vec![AgentTaskDependencyDeclaration::new(
                upstream_scope().task().clone(),
            )],
        ),
    )
    .await
    .expect("the dependent creates");
    for _round in 0..4 {
        let _ = fx.settle_task_at(&task_scope()).await;
    }

    let upstream = state_at(&fx, &upstream_scope()).await;
    assert!(
        upstream
            .task()
            .expect("the upstream exists")
            .dependents
            .is_empty(),
        "a moot registration records nothing"
    );
    let dependent = state_at(&fx, &task_scope()).await;
    let dependent_task = dependent.task().expect("the dependent exists");
    assert_eq!(dependent_task.status, AgentTaskStatus::Cancelled);
    let edge = dependent_task
        .dependencies
        .get(upstream_scope().task())
        .expect("the edge stands");
    assert_eq!(edge.outcome, Some(AgentTaskDependencyOutcome::Cancelled));
    assert!(edge.registration_settled);
}

/// A `ContinueWithEvidence` edge is satisfied by any resolution: the failed
/// upstream's outcome becomes evidence, never a cancellation.
#[tokio::test]
async fn a_continue_with_evidence_dependent_proceeds_past_the_failure() {
    let fx = fixture();
    fx.apply_task_command_at(&upstream_scope(), creation(UPSTREAM, true, Vec::new()))
        .await
        .expect("the upstream creates");
    fx.apply_task_command_at(
        &task_scope(),
        creation(
            TASK,
            true,
            vec![
                AgentTaskDependencyDeclaration::new(upstream_scope().task().clone())
                    .with_policy(AgentDependencyFailurePolicy::ContinueWithEvidence),
            ],
        ),
    )
    .await
    .expect("the dependent creates");
    for _round in 0..4 {
        let _ = fx.settle_task_at(&task_scope()).await;
        let _ = fx.settle_task_at(&upstream_scope()).await;
    }

    // The upstream is cancelled; the notification still reports it.
    fx.apply_task_command_at(
        &upstream_scope(),
        AgentTaskEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, UPSTREAM, "operator"],
            )
            .expect("the operation id derives"),
            reason: "abandoned".to_string(),
        },
    )
    .await
    .expect("the cancellation applies");
    for _round in 0..4 {
        let _ = fx.settle_task_at(&upstream_scope()).await;
        let _ = fx.settle_task_at(&task_scope()).await;
    }

    let dependent = state_at(&fx, &task_scope()).await;
    let dependent_task = dependent.task().expect("the dependent exists");
    assert_eq!(
        dependent_task.status,
        AgentTaskStatus::WaitingForInput,
        "the human dependent became eligible with the outcome as evidence"
    );
    let edge = dependent_task
        .dependencies
        .get(upstream_scope().task())
        .expect("the edge stands");
    assert_eq!(edge.outcome, Some(AgentTaskDependencyOutcome::Cancelled));
    assert!(dependent_task.cancellation.is_none());
}

/// The fencing matrix: forged initiators, unknown edges, and conflicting
/// outcomes fail closed with their exact codes, and nothing durable moves.
#[tokio::test]
async fn forged_and_conflicting_dependency_exchanges_fail_closed() {
    let fx = fixture();
    fx.apply_task_command_at(&upstream_scope(), creation(UPSTREAM, true, Vec::new()))
        .await
        .expect("the upstream creates");
    fx.apply_task_command_at(
        &task_scope(),
        creation(
            TASK,
            true,
            vec![
                AgentTaskDependencyDeclaration::new(upstream_scope().task().clone()),
                AgentTaskDependencyDeclaration::new(
                    AgentTaskId::new("upstream-2").expect("the id is valid"),
                ),
            ],
        ),
    )
    .await
    .expect("the dependent creates");
    for _round in 0..4 {
        let _ = fx.settle_task_at(&task_scope()).await;
    }

    // A registration whose initiator is not the claimed dependent is forged.
    // The claimed dependent is one that never registered — a claim over an
    // already-applied operation id is answered from the journal instead,
    // which teaches a forger nothing.
    let imposter = scope_for("imposter");
    let victim = scope_for("victim");
    let forged = registration_envelope(
        &fx,
        &victim,
        AgentEntityAddress::Task(imposter.clone()),
        upstream_scope().task(),
    );
    assert_eq!(
        deliver_to_task(&fx, &upstream_scope(), &forged).await,
        Some("dependency-registration-forged".to_string())
    );

    // A registration from a run address is forged whatever it claims.
    let second_victim = scope_for("victim-2");
    let from_run = registration_envelope(
        &fx,
        &second_victim,
        AgentEntityAddress::Run(common::run_scope()),
        upstream_scope().task(),
    );
    assert_eq!(
        deliver_to_task(&fx, &upstream_scope(), &from_run).await,
        Some("dependency-registration-forged".to_string())
    );

    // An outcome from a task that is not any recorded upstream edge is
    // unknown at the dependent. Each delivery below uses its own derived
    // operation id: the journal answers a replayed id with its first
    // decision, so every arm needs a fresh one.
    let stranger = scope_for("stranger");
    let unknown = outcome_envelope(
        &fx,
        &stranger,
        AgentEntityAddress::Task(stranger.clone()),
        &task_scope(),
        AgentTaskDependencyOutcome::Completed,
    );
    assert_eq!(
        deliver_to_task(&fx, &task_scope(), &unknown).await,
        Some("task-unknown-dependency".to_string())
    );

    // An outcome whose initiator is not the claimed upstream is forged —
    // claimed over the second edge, whose operation id no other delivery
    // has consumed.
    let second_upstream = scope_for("upstream-2");
    let misclaimed = outcome_envelope(
        &fx,
        &second_upstream,
        AgentEntityAddress::Task(stranger.clone()),
        &task_scope(),
        AgentTaskDependencyOutcome::Completed,
    );
    assert_eq!(
        deliver_to_task(&fx, &task_scope(), &misclaimed).await,
        Some("dependency-outcome-forged".to_string())
    );

    // The relay resolves the first edge; the exchange then delivering a
    // *different* outcome under its own fresh operation id is a conflict,
    // never a correction. (The same-outcome idempotent echo is proven in
    // the relay-coexistence test.)
    fx.apply_task_command_at(
        &task_scope(),
        AgentTaskEntityCommand::RecordDependencyOutcome {
            operation_id: AgentOperationId::new(
                rakka_agent::AgentOperationKind::Command,
                [TENANT, TASK, "relay-first-edge"],
            )
            .expect("the operation id derives"),
            dependency: upstream_scope().task().clone(),
            outcome: AgentTaskDependencyOutcome::Completed,
        },
    )
    .await
    .expect("the relay resolves");
    let conflicting = outcome_envelope(
        &fx,
        &upstream_scope(),
        AgentEntityAddress::Task(upstream_scope()),
        &task_scope(),
        AgentTaskDependencyOutcome::Failed,
    );
    assert_eq!(
        deliver_to_task(&fx, &task_scope(), &conflicting).await,
        Some("task-dependency-conflict".to_string())
    );

    let dependent = state_at(&fx, &task_scope()).await;
    let edge = dependent
        .task()
        .expect("the dependent exists")
        .dependencies
        .get(upstream_scope().task())
        .expect("the edge stands")
        .clone();
    assert_eq!(edge.outcome, Some(AgentTaskDependencyOutcome::Completed));
}

/// The thirty-third dependent is refused definitively; the refused
/// dependent stays `Blocked` with the refusal in its history, resolvable
/// only through the application relay.
#[tokio::test]
async fn the_dependents_ceiling_refuses_the_thirty_third_registration() {
    let fx = fixture();
    fx.apply_task_command_at(&upstream_scope(), creation(UPSTREAM, true, Vec::new()))
        .await
        .expect("the upstream creates");

    for index in 0..AGENT_TASK_MAX_DEPENDENTS {
        let dependent = scope_for(&format!("dependent-{index}"));
        let registration = registration_envelope(
            &fx,
            &dependent,
            AgentEntityAddress::Task(dependent.clone()),
            upstream_scope().task(),
        );
        assert_eq!(
            deliver_to_task(&fx, &upstream_scope(), &registration).await,
            None,
            "registration {index} records"
        );
    }
    let overflowing = scope_for("dependent-32-overflow");
    let refused = registration_envelope(
        &fx,
        &overflowing,
        AgentEntityAddress::Task(overflowing.clone()),
        upstream_scope().task(),
    );
    assert_eq!(
        deliver_to_task(&fx, &upstream_scope(), &refused).await,
        Some("task-dependents-exhausted".to_string())
    );

    let upstream = state_at(&fx, &upstream_scope()).await;
    assert_eq!(
        upstream
            .task()
            .expect("the upstream exists")
            .dependents
            .len(),
        AGENT_TASK_MAX_DEPENDENTS
    );

    // A replayed registration for a recorded dependent still answers
    // idempotently at the ceiling.
    let first = scope_for("dependent-0");
    let replay = registration_envelope(
        &fx,
        &first,
        AgentEntityAddress::Task(first.clone()),
        upstream_scope().task(),
    );
    assert_eq!(deliver_to_task(&fx, &upstream_scope(), &replay).await, None);
}

/// The application relay and the registry converge on one resolution: the
/// relay command resolves the edge first, and the exchange's later delivery
/// of the same outcome is idempotent.
#[tokio::test]
async fn the_relay_command_and_the_exchange_converge_on_one_resolution() {
    let fx = fixture();
    fx.apply_task_command_at(&upstream_scope(), creation(UPSTREAM, true, Vec::new()))
        .await
        .expect("the upstream creates");
    fx.apply_task_command_at(
        &task_scope(),
        creation(
            TASK,
            true,
            vec![AgentTaskDependencyDeclaration::new(
                upstream_scope().task().clone(),
            )],
        ),
    )
    .await
    .expect("the dependent creates");
    for _round in 0..4 {
        let _ = fx.settle_task_at(&task_scope()).await;
    }

    // The relay resolves the edge before the registry's notification runs.
    fx.apply_task_command_at(
        &task_scope(),
        AgentTaskEntityCommand::RecordDependencyOutcome {
            operation_id: AgentOperationId::new(
                rakka_agent::AgentOperationKind::Command,
                [TENANT, TASK, "relay-resolve"],
            )
            .expect("the operation id derives"),
            dependency: upstream_scope().task().clone(),
            outcome: AgentTaskDependencyOutcome::Completed,
        },
    )
    .await
    .expect("the relay resolves");

    // The registry's own notification for the same edge is idempotent.
    let exchange = outcome_envelope(
        &fx,
        &upstream_scope(),
        AgentEntityAddress::Task(upstream_scope()),
        &task_scope(),
        AgentTaskDependencyOutcome::Completed,
    );
    assert_eq!(deliver_to_task(&fx, &task_scope(), &exchange).await, None);

    let dependent = state_at(&fx, &task_scope()).await;
    let dependent_task = dependent.task().expect("the dependent exists");
    assert_eq!(dependent_task.status, AgentTaskStatus::WaitingForInput);
}

/// A pre-registry edge — persisted before the registry existed, so its
/// settled marker loads false — registers itself on the next settle pass.
#[tokio::test]
async fn a_pre_registry_edge_registers_on_the_settle_pass() {
    let fx = fixture();
    fx.apply_task_command_at(&upstream_scope(), creation(UPSTREAM, true, Vec::new()))
        .await
        .expect("the upstream creates");
    fx.apply_task_command_at(
        &task_scope(),
        creation(
            TASK,
            true,
            vec![AgentTaskDependencyDeclaration::new(
                upstream_scope().task().clone(),
            )],
        ),
    )
    .await
    .expect("the dependent creates");
    // The creation itself owed the registration; a settle pass delivers it,
    // and a *second* pass finds nothing left to owe — the settled marker
    // quiesces the derivation.
    for _round in 0..4 {
        let _ = fx.settle_task_at(&task_scope()).await;
    }
    let upstream = state_at(&fx, &upstream_scope()).await;
    assert!(upstream
        .task()
        .expect("the upstream exists")
        .dependents
        .contains_key(task_scope().task()));
    let dependent = state_at(&fx, &task_scope()).await;
    assert!(
        dependent
            .task()
            .expect("the dependent exists")
            .dependencies
            .get(upstream_scope().task())
            .expect("the edge stands")
            .registration_settled
    );
    let progress = fx
        .settle_task_at(&task_scope())
        .await
        .expect("the pass settles");
    assert_eq!(progress.outstanding, 0, "the derivation quiesced");
}
