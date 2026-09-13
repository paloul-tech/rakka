//! The run-driven communal claim append
//! ([specification 8.5 and 13.4](../../docs/plans/rakka-agent/spec.md),
//! scenario 33's rakka-agent half).
//!
//! A deduplicated `AppendClaim` command commits one idempotent `ClaimAppend`
//! effect whose provenance is stamped from durable run identity — never
//! command input — and whose space is validated against the run's delegated
//! grant at the door. The store-side idempotency lives in the graph crate's
//! conformance suite; what this file proves is the rakka-agent half: the
//! stamp, the fail-closed doors, and command replay convergence.

mod common;

use std::sync::{Arc, Mutex};

use common::{goal_spec, goal_spec_draft, goal_task_creation_command, task_definition, task_scope};
use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    claim_append_operation_id, AgentClaimAppendExecutor, AgentClaimAppendFinding,
    AgentClaimAppendProvenance, AgentClaimAppendRequest, AgentClaimObjectRequest,
    AgentCommunalClaimId, AgentDispatchFuture, AgentModelTurn, AgentModelUsage, AgentRunEffect,
    AgentRunEffectKind, AgentRunEffectOutcome, AgentRunEffectStatus, AgentRunEntityCommand,
    AgentRunEntityReply, AgentRunScope, AgentRunStatus, AgentTaskContent, KnowledgeSpaceId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};
use serde_json::json;

type Fixture = common::Fixture;

/// An executor double that records every append it was asked to perform and
/// answers with a derived claim id.
struct RecordingClaimAppendExecutor {
    seen: Mutex<Vec<(AgentClaimAppendRequest, AgentClaimAppendProvenance)>>,
}

impl RecordingClaimAppendExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
        })
    }

    fn invocations(&self) -> Vec<(AgentClaimAppendRequest, AgentClaimAppendProvenance)> {
        self.seen
            .lock()
            .expect("the append log should not be poisoned")
            .clone()
    }
}

impl AgentClaimAppendExecutor for RecordingClaimAppendExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        append: &'a AgentClaimAppendRequest,
        provenance: &'a AgentClaimAppendProvenance,
        _now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentClaimAppendFinding> {
        let mut seen = self
            .seen
            .lock()
            .expect("the append log should not be poisoned");
        seen.push((append.clone(), provenance.clone()));
        let claim = AgentCommunalClaimId::new(format!("claim-{}", seen.len()))
            .expect("the claim id is valid");
        Box::pin(async move { Ok(AgentClaimAppendFinding::Appended { claim }) })
    }
}

fn space(id: &str) -> KnowledgeSpaceId {
    KnowledgeSpaceId::new(id).expect("the space id is valid")
}

fn append_request(space_id: &str) -> AgentClaimAppendRequest {
    AgentClaimAppendRequest {
        space: space(space_id),
        subject: "finding".to_string(),
        predicate: "links".to_string(),
        object: AgentClaimObjectRequest::Value(
            AgentTaskContent::inline(json!({ "note": "observed" }))
                .expect("the object is inline-bounded"),
        ),
        confidence_bps: 5_000,
        classification: rakka_agent::MemoryClassification::Unclassified,
        evidence: Vec::new(),
        requested_by: PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "researcher".to_string(),
            display_name: None,
        },
    }
}

async fn goal_fixture(executor: Arc<RecordingClaimAppendExecutor>) -> Fixture {
    let fx = Fixture::new(ScriptedDispatcher::new().with_claim_append_executor(executor));
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.knowledge_spaces.insert(space("space-alpha"));
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the goal task creates");
    fx
}

async fn apply_append(
    fx: &Fixture,
    step: &str,
    request: AgentClaimAppendRequest,
) -> Result<AgentRunEntityReply, rakka_agent::AgentRunError> {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let scope = common::run_scope();
    run.apply(
        AgentRunEntityCommand::AppendClaim {
            operation_id: claim_append_operation_id(&scope, step)
                .expect("the operation id derives"),
            append: Box::new(request),
        },
        &fx.router,
        fx.now(),
    )
    .await
}

fn committed_append(fx_state: &rakka_agent::AgentRunState) -> Vec<AgentRunEffect> {
    fx_state
        .loop_state()
        .map(|loop_state| {
            loop_state
                .effects()
                .iter()
                .filter(|effect| effect.kind() == AgentRunEffectKind::ClaimAppendCall)
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// The command commits one effect with the provenance stamped from durable
/// run identity, a replayed command deduplicates without a second effect, and
/// the driven executor sees exactly the stamped record.
#[tokio::test]
async fn an_append_stamps_provenance_from_durable_identity_and_replays_converge() {
    let executor = RecordingClaimAppendExecutor::new();
    let fx = goal_fixture(executor.clone()).await;

    let reply = apply_append(&fx, "append-1", append_request("space-alpha"))
        .await
        .expect("the append applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));

    // The committed effect carries the transition-stamped provenance: the
    // run's own durable identity, never anything the command supplied.
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let effects = committed_append(run.state().expect("state"));
    assert_eq!(effects.len(), 1, "one command, one effect");
    let rakka_agent::AgentRunEffectRequest::ClaimAppend { provenance, .. } = &effects[0].request
    else {
        panic!("the effect carries the append request");
    };
    let scope = common::run_scope();
    assert_eq!(provenance.agent, *scope.agent());
    assert_eq!(provenance.run, *scope.run());
    assert_eq!(provenance.task, *task_scope().task());
    // The goal identity defaults to the root task's value when the creation
    // names none — the stamp carries whatever the run is durably bound to.
    assert_eq!(
        provenance.goal.as_ref().map(ToString::to_string),
        Some(common::TASK.to_string())
    );
    assert_eq!(
        provenance.delegation, None,
        "a root works under no delegation"
    );

    // A replayed command is answered from the operation log: no second
    // transition, no second effect.
    let replay = apply_append(&fx, "append-1", append_request("space-alpha"))
        .await
        .expect("the replay answers");
    assert!(matches!(replay, AgentRunEntityReply::Duplicate { .. }));
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    assert_eq!(committed_append(run.state().expect("state")).len(), 1);

    // Driving the effect reaches the executor with the stamped record; the
    // memoized answer keeps a re-driven dispatch convergent.
    fx.pump().await.expect("the effect drives");
    let seen = executor.invocations();
    assert_eq!(seen.len(), 1, "one logical append reaches the store bridge");
    assert_eq!(seen[0].0.space, space("space-alpha"));
    assert_eq!(seen[0].1.run, *scope.run());
}

/// A space outside the goal's grant refuses at the door, fail-closed, without
/// committing anything.
#[tokio::test]
async fn an_undelegated_space_refuses_at_the_door() {
    let executor = RecordingClaimAppendExecutor::new();
    let fx = goal_fixture(executor.clone()).await;

    let refused = apply_append(&fx, "append-forbidden", append_request("space-beta")).await;
    let error = refused.expect_err("the undelegated space refuses");
    assert_eq!(error.code(), "run-claim-space-not-delegated");

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    assert!(
        committed_append(run.state().expect("state")).is_empty(),
        "a refused append commits nothing"
    );
    assert!(executor.invocations().is_empty());
}

/// A delegated specialist appends only into its explicitly delegated spaces,
/// and its stamp carries the delegation identity.
#[tokio::test]
async fn a_delegated_specialist_appends_under_its_grant_with_its_delegation_stamped() {
    let executor = RecordingClaimAppendExecutor::new();
    let fx = Fixture::new(ScriptedDispatcher::new().with_claim_append_executor(executor.clone()));
    fx.instantiate_agent().await;

    let parent_run = rakka_agent::AgentRunScope::new(
        rakka_agent::TenantId::new(common::TENANT),
        rakka_agent::AgentId::new("parent-coordinator").expect("the agent id is valid"),
        rakka_agent::AgentRunId::new("parent-task-gen-1").expect("the run id is valid"),
    )
    .expect("the scope is valid");
    let delegation =
        rakka_agent::delegation_id_for(&parent_run, 1, 0).expect("the delegation id derives");
    let mut provenance = rakka_agent::AgentTaskDelegationProvenance {
        environments: Default::default(),
        knowledge_spaces: Default::default(),
        delegation: delegation.clone(),
        parent_task: rakka_agent::AgentTaskId::new("parent-task").expect("the task id is valid"),
        parent_run,
        lineage: Vec::new(),
        ancestors: Vec::new(),
        depth: 1,
        requested_skill: rakka_agent::AgentCapabilityId::new(common::SKILL)
            .expect("the capability id is valid"),
        capability_scopes: Default::default(),
        credential_bindings: Vec::new(),
        result_schema: None,
        budget: None,
        deadline: None,
    };
    provenance.knowledge_spaces.insert(space("space-granted"));

    fx.apply_task_command(rakka_agent::AgentTaskEntityCommand::Create {
        operation_id: rakka_agent::AgentOperationId::new(
            rakka_agent::AgentOperationKind::TaskCreation,
            [common::TENANT, common::TASK, "1"],
        )
        .expect("the operation id derives"),
        creation: Box::new(rakka_agent::AgentTaskCreation {
            definition: task_definition(),
            input: AgentTaskContent::inline(json!({ "text": "hello" }))
                .expect("the input is inline-bounded"),
            assignee: Some(common::agent_id()),
            team: None,
            goal: None,
            goal_mode: Default::default(),
            goal_spec: None,
            parent: Some(
                rakka_agent::AgentTaskId::new("parent-task").expect("the task id is valid"),
            ),
            dependencies: Vec::new(),
            escrow: None,
            wake: None,
            delegation: Some(Box::new(provenance)),
            telemetry: Default::default(),
        }),
    })
    .await
    .expect("the delegated task creates");

    // The grant admits the delegated space, and the stamp carries the
    // delegation that created this task.
    let reply = apply_append(&fx, "specialist-1", append_request("space-granted"))
        .await
        .expect("the delegated append applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let effects = committed_append(run.state().expect("state"));
    let rakka_agent::AgentRunEffectRequest::ClaimAppend { provenance, .. } = &effects[0].request
    else {
        panic!("the effect carries the append request");
    };
    assert_eq!(provenance.delegation, Some(delegation));

    // A space the delegation never granted fails closed.
    let refused = apply_append(&fx, "specialist-2", append_request("space-alpha")).await;
    let error = refused.expect_err("the ungranted space refuses");
    assert_eq!(error.code(), "run-claim-space-not-delegated");
}

/// An executor double that refuses every append definitively, under a code of
/// the test's choosing, so the run-side failure semantics can be observed.
struct RefusingClaimAppendExecutor {
    code: &'static str,
}

impl AgentClaimAppendExecutor for RefusingClaimAppendExecutor {
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        _append: &'a AgentClaimAppendRequest,
        _provenance: &'a AgentClaimAppendProvenance,
        _now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentClaimAppendFinding> {
        let code = self.code.to_string();
        Box::pin(async move {
            Ok(AgentClaimAppendFinding::Refused {
                code,
                message: "the claim store refused the append".to_string(),
            })
        })
    }
}

fn text_turn(text: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(text)
        .with_usage(AgentModelUsage {
            input_tokens: 8,
            output_tokens: 4,
            cost_micros: 2,
        })
}

/// A goal fixture over a scripted two-turn model, so the run holds a live
/// model wait while the append is driven and completes afterwards.
async fn scripted_goal_fixture(executor: Arc<dyn AgentClaimAppendExecutor>) -> Fixture {
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("thinking"))
            .with_turn_for(2, common::proposing_turn()),
    )
    .with_claim_append_executor(executor);
    let fx = Fixture::new(dispatcher);
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.knowledge_spaces.insert(space("space-alpha"));
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the goal task creates");
    fx
}

/// Settles the run so its committed effects are dispatchable, then records
/// `outcome` against its one outstanding claim-append effect without driving
/// anything else — the model wait the run holds is left exactly as it was.
async fn resolve_claim_append(fx: &Fixture, outcome: AgentRunEffectOutcome) -> AgentRunEffect {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("settle");
    let effect = committed_append(run.state().expect("state"))
        .into_iter()
        .find(AgentRunEffect::is_outstanding)
        .expect("one claim-append effect is outstanding");
    let scope = common::run_scope();
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(&scope)
                .expect("the result operation id derives"),
            effect_id: effect.effect_id.clone(),
            generation: effect.generation,
            attempt: effect.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(outcome),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the result applies");
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    committed_append(run.state().expect("state"))
        .into_iter()
        .find(|held| held.effect_id == effect.effect_id)
        .expect("the effect record survives its resolution")
}

/// The wind-down exemption, the claim half: a claim is a record *about* the
/// run's work, not the work, so a claim-store refusal — definitive or
/// exhausted — stays on the effect record and never fences the live run
/// (specification 13.1 and 13.4). The run keeps the wait it held, its status
/// does not move, no terminal reason is recorded, and it completes afterwards
/// exactly as it would have without the append.
#[tokio::test]
async fn a_failed_claim_append_does_not_wind_the_run_down() {
    let fx = scripted_goal_fixture(Arc::new(RefusingClaimAppendExecutor {
        code: "claim-store-refused",
    }))
    .await;
    // Crank the run to its first durable wait: the model call of turn one.
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("settle");
    let before = fx.run_snapshot().await.expect("the run exists");
    assert!(
        !before.status.is_terminal(),
        "the run holds a live wait, got {:?}",
        before.status
    );

    // A definitive refusal from the executor.
    let reply = apply_append(&fx, "append-refused", append_request("space-alpha"))
        .await
        .expect("the append applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let scope = common::run_scope();
    let (effect, request, provenance) = {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        let effect = committed_append(run.state().expect("state"))
            .into_iter()
            .next()
            .expect("the append committed");
        let rakka_agent::AgentRunEffectRequest::ClaimAppend { append, provenance } =
            &effect.request
        else {
            panic!("the effect carries the append request");
        };
        (effect.clone(), (**append).clone(), (**provenance).clone())
    };
    let outcome = fx
        .dispatcher
        .claim_append_outcome(&scope, &effect, &request, &provenance, fx.now())
        .await;
    assert!(
        matches!(&outcome, AgentRunEffectOutcome::Failed { code, .. } if code == "claim-store-refused"),
        "the executor's refusal is the effect's definitive failure, got {outcome:?}"
    );
    let failed = resolve_claim_append(&fx, outcome).await;
    assert_eq!(failed.status, AgentRunEffectStatus::Failed);
    assert_eq!(
        failed.last_error_code.as_deref(),
        Some("claim-store-refused"),
        "the failure is on the effect record"
    );
    let after = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(after.status, before.status, "the run's status did not move");
    assert_eq!(
        after.terminal_reason, None,
        "a failed append records no terminal reason"
    );

    // An exhausted retry budget, likewise.
    let reply = apply_append(&fx, "append-exhausted", append_request("space-alpha"))
        .await
        .expect("the second append applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let exhausted = resolve_claim_append(
        &fx,
        AgentRunEffectOutcome::Exhausted {
            code: "claim-store-unavailable".to_string(),
            message: "every attempt failed".to_string(),
        },
    )
    .await;
    assert_eq!(exhausted.status, AgentRunEffectStatus::Exhausted);
    assert_eq!(
        exhausted.last_error_code.as_deref(),
        Some("claim-store-unavailable")
    );
    let after = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(after.status, before.status, "the run's status did not move");
    assert_eq!(after.terminal_reason, None);

    // The run completes on its own terms afterwards: neither failure fenced
    // the model wait it held.
    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        AgentRunStatus::Completed,
        "a failed append never killed the live run, got {:?}",
        run.terminal_reason
    );
    assert!(
        !matches!(
            run.terminal_reason,
            Some(rakka_agent::AgentRunTerminalReason::EffectFailed { .. })
        ),
        "the terminal reason is the run's own, not the append's"
    );
}
