//! Autonomy admission, end to end through the agent and task entities.
//!
//! Specification: section 7.4, and scenario 53 of section 18. Unattended
//! execution (`BoundedAsync`, `Continuous`) fails closed unless an authorized
//! admission decision admits it. The enforcement is *derived* at assignment
//! from the agent's durable admission state, so these tests drive the real
//! entities — instantiate an agent, decide (or refuse) an admission, create an
//! unattended task — rather than exercising the decision type in isolation.

use std::collections::BTreeSet;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_entity_state, AgentAdmissionEvaluator, AgentAdmissionRequirement,
    AgentAssignmentRefusalReason, AgentAuthorityEnvelope, AgentBudgetCeilings, AgentDefinition,
    AgentDefinitionId, AgentEntityCommand, AgentEntityStore, AgentModelTurn, AgentModelUsage,
    AgentOperationClass, AgentOperationId, AgentOperationKind, AgentPolicyRef, AgentPolicyRefs,
    AgentRevisionNumber, AgentRunStatus, AgentSchemaPolicy, AgentSettings, AgentTaskContent,
    AgentTaskDefinition, AgentTaskStatus, AutonomyAdmissionDecision,
    AGENT_ADMISSION_DETAIL_MAX_LENGTH,
};

mod common;

use common::*;

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(rakka_agent::CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
        .with_usage(AgentModelUsage {
            input_tokens: 10,
            output_tokens: 5,
            cost_micros: 3,
        })
}

/// A task that runs unattended.
fn unattended_task() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        task_definition_id(),
        "An unattended ticket resolution.",
        schema("ticket-input"),
        schema("ticket-result"),
    )
    .expect("task definition should be valid")
    .with_operation_class(AgentOperationClass::BoundedAsync)
}

/// An agent envelope that declares the unattended task and its operation class,
/// is fully budget-bounded, and grants no tools — so an admission decision over
/// it can pass every structural requirement.
fn admittable_envelope() -> AgentAuthorityEnvelope {
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope.task_definitions.insert(task_definition_id());
    envelope
        .operation_classes
        .insert(AgentOperationClass::BoundedAsync);
    envelope.budgets = AgentBudgetCeilings {
        max_loop_iterations: Some(8),
        max_model_calls: Some(8),
        max_tool_calls: Some(8),
        max_effects: Some(8),
        max_effect_attempts: Some(16),
        max_tokens: Some(100_000),
        max_cost_micros: Some(1_000_000),
        max_wall_clock_millis: Some(600_000),
        max_concurrent_effects: Some(2),
    };
    envelope
}

fn policy(name: &str) -> AgentPolicyRef {
    AgentPolicyRef::new(name).expect("a valid policy reference")
}

/// Instantiates the agent under an envelope and policies that a `BoundedAsync`
/// admission decision can verify. Returns the envelope it was admitted under.
async fn instantiate_admittable_agent<A>(fx: &Fixture<A>) -> AgentAuthorityEnvelope
where
    A: rakka_agent::AgentModelAdapter,
{
    let envelope = admittable_envelope();
    let mut definition = AgentDefinition::new(
        AgentDefinitionId::new("unattended-v1").expect("definition id should be valid"),
        "Resolves tickets unattended.",
        envelope.clone(),
    )
    .expect("the agent definition should be valid");
    definition.policies = AgentPolicyRefs {
        approval: Some(policy("approval-v1")),
        authorization: Some(policy("authorization-v1")),
        escalation: Some(policy("escalation-v1")),
        guardrail: None,
        retention: None,
    };

    let mut agent = AgentEntityStore::new(agent_scope(), fx.agents.clone());
    agent.recover().await.expect("the agent should recover");
    agent
        .apply(AgentEntityCommand::Instantiate {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::DefinitionUpdate,
                &agent_scope(),
                "1",
            )
            .expect("operation id should be derivable"),
            definition: Box::new(definition),
            settings: Box::new(AgentSettings::default()),
            provenance: Box::new(provenance(1)),
        })
        .await
        .expect("the agent should instantiate");
    envelope
}

async fn admit<A>(fx: &Fixture<A>, decision: AutonomyAdmissionDecision)
where
    A: rakka_agent::AgentModelAdapter,
{
    let mut agent = AgentEntityStore::new(agent_scope(), fx.agents.clone());
    agent.recover().await.expect("the agent should recover");
    agent
        .apply(AgentEntityCommand::Admit {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::Command,
                &agent_scope(),
                "admit-1",
            )
            .expect("operation id should be derivable"),
            decision: Box::new(decision),
        })
        .await
        .expect("the admission is accepted");
}

fn every_requirement() -> BTreeSet<AgentAdmissionRequirement> {
    AgentAdmissionRequirement::ALL.into_iter().collect()
}

#[tokio::test]
async fn unattended_work_is_refused_without_an_admission_decision() {
    // Scenario 53's core: unattended execution fails closed when no admission
    // decision exists. The agent declares the `BoundedAsync` class and the task
    // runs under it, but nothing has admitted the agent — so the assignment is
    // refused, no run is created, and the task stays assignable so a later
    // admission can still let it through.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    fx.instantiate_agent_with_envelope(admittable_envelope())
        .await;
    fx.create_task_with(unattended_task()).await;
    fx.pump()
        .await
        .expect("the assignment is refused, not stuck");

    assert!(
        fx.run_snapshot().await.is_none(),
        "no run is created for unadmitted unattended work"
    );
    let task = fx.task_snapshot().await;
    assert_eq!(
        task.status,
        AgentTaskStatus::Created,
        "a refused task stays assignable"
    );
    let refusal = task.last_refusal.expect("a refusal is recorded");
    assert_eq!(refusal.reason, AgentAssignmentRefusalReason::NotAdmitted);
}

#[tokio::test]
async fn an_admitted_agent_runs_unattended_work() {
    // The other side of the gate: once an authorized evaluator records an
    // admission that verifies against the agent's definition, the same
    // unattended task is assigned, and the run completes. This is the only test
    // that drives the `Admit` command end to end.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    let envelope = instantiate_admittable_agent(&fx).await;

    let decision = AutonomyAdmissionDecision::new(
        [AgentOperationClass::BoundedAsync].into_iter().collect(),
        AgentRevisionNumber::INITIAL,
        AgentRevisionNumber::INITIAL,
        envelope,
        AgentAdmissionEvaluator::Service("risk-policy-service".to_string()),
        every_requirement(),
        provenance(2).accepted_at,
    )
    .expect("a complete admission");
    admit(&fx, decision).await;

    fx.create_task_with(unattended_task()).await;
    fx.pump().await.expect("the admitted run completes");

    let run = fx
        .run_snapshot()
        .await
        .expect("an admitted agent's run is created");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(fx.task_snapshot().await.status, AgentTaskStatus::Completed);
}

#[tokio::test]
async fn a_definition_that_widens_after_admission_stops_unattended_work() {
    // Scenario 53's widening clause: enforcement is *derived* against the
    // definition now in force, so a republish that widens the authority envelope
    // past what was admitted stops assignment without anything having to notice
    // the update. Here the agent is admitted for an envelope, then republishes a
    // definition that adds an operation class the admission never covered.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    let envelope = instantiate_admittable_agent(&fx).await;

    let decision = AutonomyAdmissionDecision::new(
        [AgentOperationClass::BoundedAsync].into_iter().collect(),
        AgentRevisionNumber::INITIAL,
        AgentRevisionNumber::INITIAL,
        envelope.clone(),
        AgentAdmissionEvaluator::Service("risk-policy-service".to_string()),
        every_requirement(),
        provenance(2).accepted_at,
    )
    .expect("a complete admission");
    admit(&fx, decision).await;

    // Republish a definition whose envelope widens the admitted one by adding the
    // `Continuous` class. The admission recorded against revision 1 no longer
    // covers what the agent may now do.
    let mut widened = envelope;
    widened
        .operation_classes
        .insert(AgentOperationClass::Continuous);
    let mut definition = AgentDefinition::new(
        AgentDefinitionId::new("unattended-v1").expect("definition id should be valid"),
        "Resolves tickets unattended, now continuously too.",
        widened,
    )
    .expect("the agent definition should be valid");
    definition.policies = AgentPolicyRefs {
        approval: Some(policy("approval-v1")),
        authorization: Some(policy("authorization-v1")),
        escalation: Some(policy("escalation-v1")),
        guardrail: None,
        retention: None,
    };
    let mut agent = AgentEntityStore::new(agent_scope(), fx.agents.clone());
    agent.recover().await.expect("the agent should recover");
    agent
        .apply(AgentEntityCommand::PublishDefinition {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::DefinitionUpdate,
                &agent_scope(),
                "2",
            )
            .expect("operation id should be derivable"),
            definition: Box::new(definition),
            provenance: Box::new(provenance(3)),
        })
        .await
        .expect("the widening definition publishes");

    fx.create_task_with(unattended_task()).await;
    fx.pump().await.expect("the widened assignment is refused");

    assert!(
        fx.run_snapshot().await.is_none(),
        "a widened definition stops unattended work the stale admission no longer covers"
    );
    let refusal = fx
        .task_snapshot()
        .await
        .last_refusal
        .expect("a refusal is recorded");
    assert_eq!(refusal.reason, AgentAssignmentRefusalReason::NotAdmitted);
}

#[tokio::test]
async fn a_retraction_returns_the_agent_to_the_fail_closed_default() {
    // `Retract` end to end: an admitted agent runs unattended work; a
    // retraction returns it to the fail-closed default, indistinguishable from
    // never-admitted at the enforcement point — and the retraction's bounded
    // reason lands on the entity's own durable record, so an operator asking
    // why the agent stopped running unattended gets the answer from state
    // rather than from a log.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    let envelope = instantiate_admittable_agent(&fx).await;

    let decision = AutonomyAdmissionDecision::new(
        [AgentOperationClass::BoundedAsync].into_iter().collect(),
        AgentRevisionNumber::INITIAL,
        AgentRevisionNumber::INITIAL,
        envelope,
        AgentAdmissionEvaluator::Service("risk-policy-service".to_string()),
        every_requirement(),
        provenance(2).accepted_at,
    )
    .expect("a complete admission");
    admit(&fx, decision).await;

    let mut agent = AgentEntityStore::new(agent_scope(), fx.agents.clone());
    agent.recover().await.expect("the agent should recover");

    // A reason past the bound is refused before anything durable changes: the
    // reason becomes part of a durable record, so it is held to the same bound
    // every admission detail is.
    let error = agent
        .apply(AgentEntityCommand::Retract {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::Command,
                &agent_scope(),
                "retract-too-long",
            )
            .expect("operation id should be derivable"),
            reason: "x".repeat(AGENT_ADMISSION_DETAIL_MAX_LENGTH + 1),
            provenance: Box::new(provenance(3)),
        })
        .await
        .expect_err("an unbounded reason must not enter a durable record");
    assert_eq!(error.code(), "admission-detail-too-long");

    agent
        .apply(AgentEntityCommand::Retract {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::Command,
                &agent_scope(),
                "retract-1",
            )
            .expect("operation id should be derivable"),
            reason: "credential rotation incident".to_string(),
            provenance: Box::new(provenance(3)),
        })
        .await
        .expect("the retraction is accepted");

    let state = load_agent_entity_state(&fx.agents, &agent_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the agent state loads")
        .expect("the agent exists");
    assert!(
        state.admission().is_none(),
        "a retracted admission is gone from every enforcement point"
    );
    let retraction = state
        .admission_retraction()
        .expect("the retraction explains the fail-closed default");
    assert_eq!(retraction.reason, "credential rotation incident");

    fx.create_task_with(unattended_task()).await;
    fx.pump()
        .await
        .expect("the assignment is refused, not stuck");

    assert!(
        fx.run_snapshot().await.is_none(),
        "a retracted agent runs no unattended work"
    );
    let refusal = fx
        .task_snapshot()
        .await
        .last_refusal
        .expect("a refusal is recorded");
    assert_eq!(refusal.reason, AgentAssignmentRefusalReason::NotAdmitted);
}

#[tokio::test]
async fn an_undeclared_operation_class_is_refused_as_its_own_reason() {
    // The envelope declares the task's definition but not the unattended
    // class the task runs under. The refusal names the class, not the task
    // definition, because the remedies differ: the definition is fully
    // declared, and it is the *kind of autonomy* the envelope never granted.
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(proposing_turn("resolved")));
    let mut envelope = admittable_envelope();
    envelope.operation_classes.clear();
    fx.instantiate_agent_with_envelope(envelope).await;

    fx.create_task_with(unattended_task()).await;
    fx.pump()
        .await
        .expect("the assignment is refused, not stuck");

    assert!(
        fx.run_snapshot().await.is_none(),
        "an undeclared operation class admits nothing"
    );
    let refusal = fx
        .task_snapshot()
        .await
        .last_refusal
        .expect("a refusal is recorded");
    assert_eq!(
        refusal.reason,
        AgentAssignmentRefusalReason::OperationClassNotDeclared
    );
}
