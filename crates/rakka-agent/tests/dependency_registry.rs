//! The dependents registry
//! ([specification 9.2](../../../docs/plans/rakka-agent/spec.md), the sending
//! half deferred from slice 4.6 to 5.4): a dependent registers its forward
//! edge with the upstream in the same compare-and-set that declares it, the
//! upstream walks its bounded registry at terminalization to owe each
//! dependent its outcome, and every leg is sender-fenced, deduplicated, and
//! answerable past the journal's bounded window.

mod common;

use std::sync::Arc;

use common::{task_definition, task_scope, tenant, Fixture, TASK, TENANT};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    dependency_outcome_operation_id, dependency_registration_operation_id, load_agent_task_state,
    AgentDependencyFailurePolicy, AgentDependencyOutcomeNotice, AgentDependencyRegistration,
    AgentEntityAddress, AgentExchangeEnvelope, AgentExchangeKind, AgentExchangePayload,
    AgentExchangeState, AgentOperationId, AgentSchemaPolicy, AgentTaskContent, AgentTaskCreation,
    AgentTaskDependencyDeclaration, AgentTaskDependencyOutcome, AgentTaskEntityCommand,
    AgentTaskEntityStore, AgentTaskId, AgentTaskOwnership, AgentTaskScope, AgentTaskState,
    AgentTaskStatus, AGENT_DEPENDENCY_OUTCOME_PAYLOAD_TYPE,
    AGENT_DEPENDENCY_REGISTRATION_PAYLOAD_TYPE, AGENT_TASK_MAX_DEPENDENCIES,
    AGENT_TASK_MAX_DEPENDENTS, METRIC_AGENT_EXCHANGE_UNSETTLEABLE,
};
use rakka_agent_workflow::AgentCorrelationId;
use rakka_core::InMemoryMetricsRecorder;
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

/// A dependent created before its upstream converges: the registration is
/// refused `task-not-created`, which the upstream does *not* memoize, so the
/// next re-drive re-runs the arm against the now-created task.
#[tokio::test]
async fn a_registration_racing_its_upstreams_creation_converges() {
    let fx = fixture();
    // The dependent goes first, so its registration lands at a task that does
    // not exist yet.
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
    let dependent = state_at(&fx, &task_scope()).await;
    assert!(
        !dependent
            .task()
            .expect("the dependent exists")
            .dependencies
            .get(upstream_scope().task())
            .expect("the edge stands")
            .registration_settled,
        "the refusal is retryable, so nothing settled"
    );

    // The upstream is created a moment later. Nothing re-declares the edge:
    // the still-outstanding registration is simply re-driven.
    fx.apply_task_command_at(&upstream_scope(), creation(UPSTREAM, true, Vec::new()))
        .await
        .expect("the upstream creates");
    for _round in 0..4 {
        let _ = fx.settle_task_at(&task_scope()).await;
    }

    let upstream = state_at(&fx, &upstream_scope()).await;
    assert!(
        upstream
            .task()
            .expect("the upstream exists")
            .dependents
            .contains_key(task_scope().task()),
        "the re-drive registered the dependent"
    );
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

/// A permanently unanswerable exchange is *reported*, not swallowed. The pass
/// still succeeds — one stuck envelope must never wedge the other exchanges an
/// entity owes — but the caller can tell a durably wedged entity from a
/// healthy one, and standing wedged stops costing a durable write once the
/// refusal is recorded.
#[tokio::test]
async fn a_permanently_unanswerable_exchange_is_reported_not_swallowed() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new(),
    ))
    .with_metrics(metrics.clone());
    // The upstream is never created, so the registration is answered
    // `task-not-created` — the class `check_settle` leaves outstanding
    // because only a future receiver could resolve it.
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

    // The creation's own settle pass already met the wedge, so the loop is
    // measured as a delta.
    let before = metrics.snapshot();
    let counted_before = before
        .observations_named(METRIC_AGENT_EXCHANGE_UNSETTLEABLE)
        .len();
    assert_eq!(counted_before, 1, "the creation's own pass counted it once");

    const ROUNDS: usize = 4;
    for round in 0..ROUNDS {
        let progress = fx
            .settle_task_at(&task_scope())
            .await
            .expect("one unanswerable envelope does not fail the pass");
        assert_eq!(progress.unsettleable, 1, "round {round}");
        assert_eq!(progress.outstanding, 1, "round {round}");
    }

    let observations = metrics.snapshot();
    assert_eq!(
        observations
            .observations_named(METRIC_AGENT_EXCHANGE_UNSETTLEABLE)
            .len()
            - counted_before,
        ROUNDS,
        "a standing wedge counts on every sweep, so it is alertable as a rate"
    );

    // The refusal is durably legible, and re-driving it costs nothing further:
    // the attempt counter moved exactly once, not once per pass.
    let state = state_at(&fx, &task_scope()).await;
    let pending = state.exchange_journal().outstanding();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].last_failure_code(), Some("task-not-created"));
    assert_eq!(
        pending[0].attempts(),
        1,
        "an unchanged refusal writes nothing further"
    );
}

/// A registration the upstream refuses definitively resolves the dependent's
/// own edge: no registry entry exists, so no notification can ever arrive,
/// and the edge's declared policy decides what that means rather than the
/// dependent waiting on an answer that cannot come.
#[tokio::test]
async fn a_registration_refused_at_the_ceiling_resolves_the_dependents_edge() {
    let fx = fixture();
    fx.apply_task_command_at(&upstream_scope(), creation(UPSTREAM, true, Vec::new()))
        .await
        .expect("the upstream creates");
    for index in 0..AGENT_TASK_MAX_DEPENDENTS {
        let dependent = scope_for(&format!("filler-{index}"));
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

    // A real dependent now declares the edge against the full registry.
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

    let dependent = state_at(&fx, &task_scope()).await;
    let dependent_task = dependent.task().expect("the dependent exists");
    let edge = dependent_task
        .dependencies
        .get(upstream_scope().task())
        .expect("the edge stands");
    assert!(edge.registration_settled);
    assert_eq!(
        edge.outcome,
        Some(AgentTaskDependencyOutcome::Failed),
        "an unregisterable edge resolves failed rather than hanging"
    );
    assert_eq!(
        dependent_task.status,
        AgentTaskStatus::Cancelled,
        "the declared cancel-dependents policy applied"
    );
    let progress = fx
        .settle_task_at(&task_scope())
        .await
        .expect("the pass settles");
    assert_eq!(progress.outstanding, 0, "nothing is left owed");
}

/// A task whose forward edges and dependents both sit at their ceilings
/// terminalizes and notifies every one of them: the moot registrations a
/// terminal task can no longer act on are withdrawn, so the two bounded
/// sides of the graph never contend for the journal's single pending list.
#[tokio::test]
async fn a_terminal_task_notifies_every_dependent_past_the_pending_bound() {
    let fx = fixture();
    let dependencies: Vec<AgentTaskDependencyDeclaration> = (0..AGENT_TASK_MAX_DEPENDENCIES)
        .map(|index| {
            AgentTaskDependencyDeclaration::new(
                AgentTaskId::new(format!("upstream-{index}")).expect("the task id is valid"),
            )
        })
        .collect();
    fx.apply_task_command_at(&task_scope(), creation(TASK, true, dependencies))
        .await
        .expect("the task creates");
    // None of the upstreams exist, so every registration is refused
    // `task-not-created` and stays outstanding — the documented posture.
    for _round in 0..4 {
        let _ = fx.settle_task_at(&task_scope()).await;
    }
    for index in 0..AGENT_TASK_MAX_DEPENDENTS {
        let dependent = scope_for(&format!("dependent-{index}"));
        let registration = registration_envelope(
            &fx,
            &dependent,
            AgentEntityAddress::Task(dependent.clone()),
            task_scope().task(),
        );
        assert_eq!(
            deliver_to_task(&fx, &task_scope(), &registration).await,
            None,
            "registration {index} records"
        );
    }

    fx.apply_task_command_at(
        &task_scope(),
        AgentTaskEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, TASK, "operator"],
            )
            .expect("the operation id derives"),
            reason: "abandoned".to_string(),
        },
    )
    .await
    .expect("a full dependency graph does not block terminalization");

    let mut progress = None;
    for _round in 0..8 {
        progress = fx.settle_task_at(&task_scope()).await.ok();
    }
    assert_eq!(
        progress.expect("the pass settles").outstanding,
        0,
        "every notification settled and no moot registration is left owed"
    );
    let task_state = state_at(&fx, &task_scope()).await;
    let task = task_state.task().expect("the task exists");
    assert_eq!(task.status, AgentTaskStatus::Cancelled);
    assert_eq!(task.dependents.len(), AGENT_TASK_MAX_DEPENDENTS);
    assert!(
        task.dependents
            .values()
            .all(|record| record.outcome_settled),
        "every registered dependent was notified"
    );
}

/// The thirty-third dependent is refused definitively, and the refusal is
/// recorded at the upstream with its exact code.
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

/// A world with the whole registry round trip ahead of it: a human-owned
/// upstream and one dependent that declared its edge at creation.
async fn registry_world() -> Fixture {
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
    fx
}

/// Drives both legs to quiescence, tolerating an injected loss: the forward
/// registration reaches the upstream, the upstream terminalizes, and the
/// outcome notification comes back. Errors are swallowed because a crashed
/// owner is supposed to fail here; convergence is asserted from durable state
/// afterwards.
async fn drive_registry(fx: &Fixture) {
    for _round in 0..4 {
        let _ = fx.settle_task_at(&task_scope()).await;
        let _ = fx.settle_task_at(&upstream_scope()).await;
    }
    let _ = fx
        .apply_task_command_at(
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
        .await;
    for _round in 0..8 {
        let _ = fx.settle_task_at(&upstream_scope()).await;
        let _ = fx.settle_task_at(&task_scope()).await;
    }
}

/// The convergence property of both registry legs, from durable state alone.
///
/// The two `settled` markers are the once-guards that quiesce the derivations
/// past the journal's bounded window, so a re-driving owner must land on
/// exactly one edge, one registration, and one outcome — never a second
/// dependent record, and never a dependent left blocked on an upstream that
/// already ended.
async fn assert_registry_converged(fx: &Fixture, context: &str) {
    let upstream_state = state_at(fx, &upstream_scope()).await;
    let upstream = upstream_state.task().expect("the upstream exists");
    assert_eq!(
        upstream.status,
        AgentTaskStatus::Cancelled,
        "{context}: the upstream reached its terminal"
    );
    assert_eq!(
        upstream.dependents.len(),
        1,
        "{context}: exactly one dependent record, however often the edge re-registered"
    );
    let record = upstream
        .dependents
        .get(task_scope().task())
        .unwrap_or_else(|| panic!("{context}: the registered dependent is the one that asked"));
    assert!(
        record.outcome_settled,
        "{context}: the upstream owes its dependent nothing further"
    );

    let dependent_state = state_at(fx, &task_scope()).await;
    let dependent = dependent_state.task().expect("the dependent exists");
    assert_eq!(
        dependent.dependencies.len(),
        1,
        "{context}: exactly one dependency edge"
    );
    let edge = dependent
        .dependencies
        .get(upstream_scope().task())
        .unwrap_or_else(|| panic!("{context}: the declared edge stands"));
    assert!(
        edge.registration_settled,
        "{context}: the forward edge registered exactly once"
    );
    assert!(
        edge.outcome.is_some(),
        "{context}: the dependent learned its upstream's outcome rather than blocking forever"
    );

    let progress = fx
        .settle_task_at(&task_scope())
        .await
        .expect("the dependent's pass settles");
    assert_eq!(
        progress.outstanding, 0,
        "{context}: the dependent's derivations quiesced"
    );
    let progress = fx
        .settle_task_at(&upstream_scope())
        .await
        .expect("the upstream's pass settles");
    assert_eq!(
        progress.outstanding, 0,
        "{context}: the upstream's derivations quiesced"
    );
}

/// The durable writes one crash-free registry round trip attempts, so the
/// sweep below covers every real write rather than a guess.
async fn reference_writes() -> usize {
    let fx = registry_world().await;
    fx.tasks.reset_writes();
    drive_registry(&fx).await;
    assert_registry_converged(&fx, "the crash-free reference").await;
    fx.tasks.writes()
}

#[tokio::test]
async fn both_registry_legs_converge_across_every_task_store_crash_point() {
    // Slice 5.6's fault-injection half of the 5.4 registry
    // ([specification 15 and 18](../../../docs/plans/rakka-agent/spec.md)).
    // Both tasks share one store, so one armed store covers the forward
    // registration, the upstream's terminal commit, and the outcome
    // notification — every compare-and-set the registry introduced.
    let writes = reference_writes().await;
    assert!(
        writes >= 4,
        "the round trip writes the task store at least four times \
         (registration owed, registration recorded, terminal commit, outcome settled), \
         saw {writes}"
    );

    for point in 1..=writes {
        for window in [
            rakka_agent::testkit::CrashPoint::BeforeWrite,
            rakka_agent::testkit::CrashPoint::AfterWrite,
        ] {
            let fx = registry_world().await;
            fx.tasks.reset_writes();
            fx.tasks.crash_at(point, window);
            drive_registry(&fx).await;
            fx.tasks.assert_crash_fired(point, window);
            fx.tasks.survive();

            // A new owner, with nothing but the durable record.
            drive_registry(&fx).await;
            assert_registry_converged(&fx, &format!("crash at write {point} ({window:?})")).await;
        }
    }
}

#[tokio::test]
async fn both_registry_exchanges_survive_every_delivery_fault() {
    // The fifteenth and sixteenth exchanges' own failure windows, at the real
    // entity rather than through the synthetic choreography probe. Each leg is
    // re-derived by every settle pass and guarded past the journal window by
    // its own settled marker, so a lost envelope, a lost reply, and a doubled
    // delivery must all land on one edge and one notification.
    for fault in [
        rakka_agent::testkit::ExchangeFault::LoseEnvelope,
        rakka_agent::testkit::ExchangeFault::LoseReply,
        rakka_agent::testkit::ExchangeFault::DeliverTwice,
    ] {
        let fx = registry_world().await;
        fx.task_transport.inject(fault);
        drive_registry(&fx).await;
        assert_registry_converged(&fx, &format!("{fault:?}")).await;
    }
}
