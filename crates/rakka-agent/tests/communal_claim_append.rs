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
    agent_operational_snapshot, claim_append_operation_id, AgentCheckpoint, AgentCheckpointKind,
    AgentClaimAppendExecutor, AgentClaimAppendFinding, AgentClaimAppendProvenance,
    AgentClaimAppendRequest, AgentClaimObjectRequest, AgentCommunalClaimId, AgentDispatchFuture,
    AgentModelTurn, AgentModelUsage, AgentOperationId, AgentOperationKind, AgentRunEffect,
    AgentRunEffectKind, AgentRunEffectOutcome, AgentRunEffectStatus, AgentRunEntityCommand,
    AgentRunEntityReply, AgentRunScope, AgentRunStatus, AgentSchemaPolicy, AgentTaskContent,
    AgentToolCallId, AgentToolCallRequest, AgentToolId, KnowledgeSpaceId,
    AGENT_POST_TERMINAL_MEMORY_WINDOW_DEFAULT_MS, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
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
            ..Default::default()
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
        AgentRunEffectOutcome::exhausted(
            "claim-store-unavailable".to_string(),
            "every attempt failed".to_string(),
        ),
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

// ===========================================================================
// The post-terminal window, the claim half: a run that has ended still
// accepts an append for a bounded window, the effect rides the ordinary
// outbox, and its outcome lands on the terminal record without moving it
// (specification 13.4; scenario 33's rakka-agent half, after the run's end).
// ===========================================================================

/// How the world's run ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    Completed,
    Failed,
    Cancelled,
}

impl Ending {
    const ALL: [Self; 3] = [Self::Completed, Self::Failed, Self::Cancelled];

    fn status(self) -> AgentRunStatus {
        match self {
            Self::Completed => AgentRunStatus::Completed,
            Self::Failed => AgentRunStatus::Failed,
            Self::Cancelled => AgentRunStatus::Cancelled,
        }
    }
}

fn tool_calling_turn(tool: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look that up.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id"),
                AgentToolId::new(tool).expect("tool id"),
                serde_json::json!({ "query": "ticket" }),
            )
            .expect("the tool call is bounded"),
        )
        .with_usage(AgentModelUsage {
            input_tokens: 8,
            output_tokens: 4,
            cost_micros: 2,
            ..Default::default()
        })
}

/// A goal world with the recording executor, driven to its terminal status.
async fn ended_goal_world(ending: Ending, executor: Arc<RecordingClaimAppendExecutor>) -> Fixture {
    let adapter = match ending {
        Ending::Failed => {
            DeterministicModelAdapter::new().with_turn_for(1, tool_calling_turn("flaky-tool"))
        }
        Ending::Completed | Ending::Cancelled => DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("thinking"))
            .with_turn_for(2, common::proposing_turn()),
    };
    let mut dispatcher =
        ScriptedDispatcher::with_adapter(adapter).with_claim_append_executor(executor);
    if ending == Ending::Failed {
        dispatcher =
            dispatcher.with_tool_failure("flaky-tool", "tool-unavailable", "the tool is down");
    }
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
    if ending == Ending::Cancelled {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        run.settle_side_effects(&fx.router, fx.now())
            .await
            .expect("crank to the model wait");
        run.apply(
            AgentRunEntityCommand::Cancel {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::Cancellation,
                    [common::TENANT, common::AGENT, "1"],
                )
                .expect("derivable"),
                reason: "operator stopped the work".to_string(),
            },
            &fx.router,
            fx.now(),
        )
        .await
        .expect("the cancel applies");
    }
    fx.pump().await.expect("the loop runs to its end");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, ending.status(), "the world ended as scripted");
    assert!(
        run.terminal_at.is_some(),
        "the terminal transition stamped the run"
    );
    fx
}

/// Inside the window, a run that ended `Completed`, `Failed`, or `Cancelled`
/// accepts an append: the effect is committed, rides the ordinary outbox,
/// reaches the executor once, and its outcome lands on the terminal record
/// — whose status, terminal reason, and terminal stamp do not move.
#[tokio::test]
async fn a_claim_append_is_accepted_inside_the_post_terminal_window() {
    for ending in Ending::ALL {
        let executor = RecordingClaimAppendExecutor::new();
        let fx = ended_goal_world(ending, executor.clone()).await;
        let before = fx.run_snapshot().await.expect("the run exists");

        let reply = apply_append(&fx, "post-terminal", append_request("space-alpha"))
            .await
            .unwrap_or_else(|error| {
                panic!("{ending:?}: the post-terminal append applies: {error}")
            });
        assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
        fx.pump_post_terminal(AgentRunEffectKind::ClaimAppendCall)
            .await
            .unwrap_or_else(|error| panic!("{ending:?}: {error}"));

        assert_eq!(
            executor.invocations().len(),
            1,
            "{ending:?}: the append reached the store bridge once"
        );
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        let effects = committed_append(run.state().expect("state"));
        assert_eq!(effects.len(), 1, "{ending:?}: one effect");
        assert_eq!(
            effects[0].status,
            AgentRunEffectStatus::Succeeded,
            "{ending:?}: the append succeeded"
        );
        let after = fx.run_snapshot().await.expect("the run exists");
        assert_eq!(
            after.status, before.status,
            "{ending:?}: the status did not move"
        );
        assert_eq!(
            after.terminal_reason, before.terminal_reason,
            "{ending:?}: the terminal reason did not move"
        );
        assert_eq!(
            after.terminal_at, before.terminal_at,
            "{ending:?}: the terminal stamp did not move"
        );
    }
}

/// An ambiguous claim-append outcome delivered to an ended run parks the
/// append `Indeterminate` and opens its reconciliation checkpoint, without
/// `compensate`, and the terminal record does not move.
#[tokio::test]
async fn an_ended_runs_ambiguous_claim_append_opens_its_reconciliation_checkpoint() {
    let executor = RecordingClaimAppendExecutor::new();
    let fx = ended_goal_world(Ending::Completed, executor.clone()).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let reply = apply_append(&fx, "ambiguous", append_request("space-alpha"))
        .await
        .expect("the post-terminal append applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let parked = resolve_claim_append(
        &fx,
        AgentRunEffectOutcome::indeterminate(
            "dispatcher-lost-after-started",
            "the recovery retry was refused",
        ),
    )
    .await;
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let open = run
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .open_checkpoints();
    assert_eq!(open.len(), 1, "one checkpoint opens");
    assert_eq!(
        open[0].kind,
        AgentCheckpointKind::IndeterminateEffectReconciliation
    );
    assert_eq!(
        open[0].checkpoint_id,
        AgentCheckpoint::id_for_effect(
            &parked.effect_id,
            parked.generation,
            AgentCheckpointKind::IndeterminateEffectReconciliation
        )
    );
    let decisions: Vec<&str> = open[0]
        .allowed_decisions
        .iter()
        .map(|decision| decision.value.as_str())
        .collect();
    assert_eq!(
        decisions,
        [
            "confirmed-completed",
            "confirmed-not-executed",
            "escalate",
            "abandon-and-fail"
        ]
    );
    let snapshot = agent_operational_snapshot(
        &fx.runs,
        &common::run_scope(),
        &AgentSchemaPolicy::default(),
        fx.now(),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    assert_eq!(snapshot.open_checkpoints.len(), 1);

    let after = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(after.status, before.status);
    assert_eq!(after.terminal_reason, before.terminal_reason);
    assert_eq!(after.terminal_at, before.terminal_at);
}

/// `ConfirmedNotExecuted` on an ended run re-sends its parked claim append
/// under a new generation, and so a new idempotency key: the append lands
/// through the ordinary outbox and the terminal record does not move.
#[tokio::test]
async fn confirmed_not_executed_re_sends_an_ended_runs_claim_append_under_a_new_key() {
    let executor = RecordingClaimAppendExecutor::new();
    let fx = ended_goal_world(Ending::Completed, executor.clone()).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    apply_append(&fx, "ambiguous", append_request("space-alpha"))
        .await
        .expect("the post-terminal append applies");
    let parked = resolve_claim_append(
        &fx,
        AgentRunEffectOutcome::indeterminate(
            "dispatcher-lost-after-started",
            "the recovery retry was refused",
        ),
    )
    .await;
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    let first_key = parked
        .safety
        .external_key()
        .expect("an append carries its external key")
        .as_str()
        .to_string();
    assert!(first_key.ends_with("#g1"), "{first_key}");

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.apply(
        AgentRunEntityCommand::ResolveIndeterminateEffect {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &common::agent_scope(),
                "not-executed",
            )
            .expect("the decision key derives"),
            effect_id: parked.effect_id.clone(),
            generation: parked.generation,
            resolution: Box::new(rakka_agent::AgentEffectResolution::ConfirmedNotExecuted),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("an ended run's parked append is re-sent");

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let resent = committed_append(run.state().expect("state"))
        .into_iter()
        .find(|held| held.effect_id == parked.effect_id)
        .expect("the effect survives");
    assert_eq!(resent.generation, parked.generation.next());
    let key = resent
        .safety
        .external_key()
        .expect("the new generation carries a key")
        .as_str();
    assert!(
        key.ends_with("#g2"),
        "the new generation derives a new key: {key}"
    );
    assert!(run
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .open_checkpoints()
        .is_empty());
    assert!(executor.invocations().is_empty(), "nothing sent yet");

    fx.pump_post_terminal(AgentRunEffectKind::ClaimAppendCall)
        .await
        .expect("the re-sent append settles");
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let done = committed_append(run.state().expect("state"))
        .into_iter()
        .find(|held| held.effect_id == parked.effect_id)
        .expect("the effect survives");
    assert_eq!(done.status, AgentRunEffectStatus::Succeeded);
    assert_eq!(executor.invocations().len(), 1);

    let after = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(after.status, before.status);
    assert_eq!(after.terminal_reason, before.terminal_reason);
    assert_eq!(after.terminal_at, before.terminal_at);
}

/// A person's `ConfirmedCompleted` through `ResolveCheckpoint` answers an
/// ended run's parked claim append: the outcome lands on the same generation,
/// the checkpoint is gone, and the terminal record never moves.
#[tokio::test]
async fn a_checkpoint_decision_answers_an_ended_runs_claim_append() {
    let executor = RecordingClaimAppendExecutor::new();
    let fx = ended_goal_world(Ending::Completed, executor.clone()).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    apply_append(&fx, "ambiguous", append_request("space-alpha"))
        .await
        .expect("the post-terminal append applies");
    let parked = resolve_claim_append(
        &fx,
        AgentRunEffectOutcome::indeterminate(
            "dispatcher-lost-after-started",
            "the recovery retry was refused",
        ),
    )
    .await;
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let checkpoint_id = {
        let open = run
            .state()
            .expect("state")
            .loop_state()
            .expect("the loop is started")
            .open_checkpoints();
        assert_eq!(open.len(), 1, "one checkpoint opens");
        open[0].checkpoint_id.clone()
    };
    let unmoved = |after: rakka_agent::AgentRunSnapshot, at: &str| {
        assert_eq!(after.status, before.status, "{at}: status");
        assert_eq!(
            after.terminal_reason, before.terminal_reason,
            "{at}: reason"
        );
        assert_eq!(after.terminal_at, before.terminal_at, "{at}: stamp");
    };
    unmoved(
        fx.run_snapshot().await.expect("the run exists"),
        "after the park",
    );

    run.apply(
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &common::agent_scope(),
                "completed",
            )
            .expect("the decision key derives"),
            checkpoint_id,
            resolver: PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "operator".to_string(),
                display_name: None,
            },
            decision: Box::new(rakka_agent::AgentCheckpointDecision::Reconciliation(
                rakka_agent::AgentReconciliationDecision::ConfirmedCompleted {
                    resolution: Box::new(rakka_agent::AgentEffectResolution::ConfirmedExecuted {
                        outcome: Box::new(AgentRunEffectOutcome::ClaimAppend {
                            claim: AgentCommunalClaimId::new("claim-established")
                                .expect("the claim id is valid"),
                        }),
                    }),
                },
            )),
            telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("an ended run's parked append is answered through its checkpoint");

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let done = committed_append(run.state().expect("state"))
        .into_iter()
        .find(|held| held.effect_id == parked.effect_id)
        .expect("the effect survives");
    assert_eq!(done.status, AgentRunEffectStatus::Succeeded);
    assert_eq!(done.generation, parked.generation);
    assert!(run
        .state()
        .expect("state")
        .loop_state()
        .expect("the loop is started")
        .open_checkpoints()
        .is_empty());
    assert!(executor.invocations().is_empty(), "nothing was re-sent");
    unmoved(
        fx.run_snapshot().await.expect("the run exists"),
        "after the decision",
    );
}

/// The window is inclusive of its last millisecond and closed one past it,
/// under `run-memory-window-closed`, with nothing committed and the executor
/// never reached.
#[tokio::test]
async fn a_claim_append_past_the_window_is_refused_and_commits_nothing() {
    let executor = RecordingClaimAppendExecutor::new();
    let fx = ended_goal_world(Ending::Completed, executor.clone()).await;
    let terminal_at = fx
        .run_snapshot()
        .await
        .expect("the run exists")
        .terminal_at
        .expect("stamped")
        .as_millis();
    let window = AGENT_POST_TERMINAL_MEMORY_WINDOW_DEFAULT_MS;

    // `apply_append` reads the clock twice — once to recover, once to apply —
    // so the transition sees the stored value plus one.
    fx.clock.store(
        terminal_at + window - 1,
        std::sync::atomic::Ordering::SeqCst,
    );
    let reply = apply_append(&fx, "boundary", append_request("space-alpha"))
        .await
        .expect("an append at the boundary applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    fx.pump_post_terminal(AgentRunEffectKind::ClaimAppendCall)
        .await
        .expect("the boundary append settles");
    assert_eq!(executor.invocations().len(), 1);

    fx.clock
        .store(terminal_at + window, std::sync::atomic::Ordering::SeqCst);
    let error = apply_append(&fx, "late", append_request("space-alpha"))
        .await
        .expect_err("an append past the window is refused");
    assert_eq!(error.code(), "run-memory-window-closed");
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    assert_eq!(
        committed_append(run.state().expect("state")).len(),
        1,
        "the refused append committed no effect"
    );
    assert_eq!(executor.invocations().len(), 1, "and reached no executor");
}

/// A zero window closes the door under the plain terminal refusal.
#[tokio::test]
async fn a_zero_window_restores_the_terminal_refusal_for_claims() {
    let executor = RecordingClaimAppendExecutor::new();
    let mut fx = ended_goal_world(Ending::Completed, executor.clone()).await;
    fx.policies = fx.policies.clone().with_post_terminal_memory_window_ms(0);
    let error = apply_append(&fx, "closed", append_request("space-alpha"))
        .await
        .expect_err("a zero window refuses");
    assert_eq!(error.code(), "run-terminal");
    assert!(executor.invocations().is_empty());
}

/// Replay after the run's end writes once: the command replay answers from
/// the operation log with one effect, and the result replay deduplicates.
#[tokio::test]
async fn a_post_terminal_claim_append_replays_once() {
    let executor = RecordingClaimAppendExecutor::new();
    let fx = ended_goal_world(Ending::Completed, executor.clone()).await;
    let reply = apply_append(&fx, "post-1", append_request("space-alpha"))
        .await
        .expect("the append applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let replay = apply_append(&fx, "post-1", append_request("space-alpha"))
        .await
        .expect("the replay answers");
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "the replayed command deduplicates: {replay:?}"
    );
    fx.pump_post_terminal(AgentRunEffectKind::ClaimAppendCall)
        .await
        .expect("the append settles");
    assert_eq!(executor.invocations().len(), 1);

    // The result replay: the memoized executor answer is re-recorded under
    // the same result operation id and deduplicates.
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let effect = committed_append(run.state().expect("state"))
        .into_iter()
        .next()
        .expect("the effect record survives");
    assert_eq!(effect.status, AgentRunEffectStatus::Succeeded);
    let rakka_agent::AgentRunEffectRequest::ClaimAppend { append, provenance } = &effect.request
    else {
        panic!("the effect carries the append request");
    };
    let scope = common::run_scope();
    let outcome = fx
        .dispatcher
        .claim_append_outcome(&scope, &effect, append, provenance, fx.now())
        .await;
    let second = run
        .apply(
            AgentRunEntityCommand::RecordEffectResult {
                operation_id: effect
                    .result_operation_id(&scope)
                    .expect("the result operation id derives"),
                effect_id: effect.effect_id.clone(),
                generation: effect.generation,
                attempt: effect.attempts,
                fence: 0,
                outcome: Box::new(outcome),
            },
            &fx.router,
            fx.now(),
        )
        .await
        .expect("the replayed result answers");
    assert!(
        matches!(second, AgentRunEntityReply::Duplicate { .. }),
        "the replayed result deduplicates: {second:?}"
    );
    assert_eq!(
        executor.invocations().len(),
        1,
        "the memo answered, not the store"
    );
}

/// An exhausted post-terminal append, being exempt from the wind-down, ends
/// nothing: the terminal record is untouched.
#[tokio::test]
async fn an_exhausted_post_terminal_claim_append_leaves_the_terminal_record_untouched() {
    let executor = RecordingClaimAppendExecutor::new();
    let fx = ended_goal_world(Ending::Failed, executor.clone()).await;
    let before = fx.run_snapshot().await.expect("the run exists");
    let reply = apply_append(&fx, "exhausted", append_request("space-alpha"))
        .await
        .expect("the append applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let exhausted = resolve_claim_append(
        &fx,
        AgentRunEffectOutcome::exhausted(
            "claim-store-unavailable".to_string(),
            "every attempt failed".to_string(),
        ),
    )
    .await;
    assert_eq!(exhausted.status, AgentRunEffectStatus::Exhausted);
    let after = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(after.status, before.status);
    assert_eq!(after.terminal_reason, before.terminal_reason);
    assert_eq!(after.terminal_at, before.terminal_at);
    assert!(executor.invocations().is_empty());
}

// ===========================================================================
// A turn rests on the turn's own effects (gap slice 3), the claim half: an
// append outstanding when the turn's last effect result lands never holds
// the turn open, and its own outcome never rests one.
// ===========================================================================

/// Where the run durably rests: phase, status, turn, and how many effects of
/// any kind are outstanding.
async fn resting_at(fx: &Fixture) -> (rakka_agent::AgentLoopPhase, AgentRunStatus, u64, usize) {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is started");
    (
        loop_state.phase(),
        state.status().expect("the run exists"),
        loop_state.turn(),
        loop_state.outstanding_effects().count(),
    )
}

/// The one dispatched, still-outstanding effect of `kind` the run holds.
async fn dispatched_effect(fx: &Fixture, kind: AgentRunEffectKind) -> AgentRunEffect {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let state = run.state().expect("state");
    let effects: Vec<AgentRunEffect> = state
        .loop_state()
        .expect("the loop is started")
        .effects()
        .iter()
        .filter(|effect| effect.kind() == kind && effect.status == AgentRunEffectStatus::Ready)
        .cloned()
        .collect();
    assert_eq!(effects.len(), 1, "exactly one dispatched {kind} effect");
    effects[0].clone()
}

/// Records the dispatcher's scripted answer to exactly one effect and nothing
/// else, so a test chooses which result lands first.
async fn answer_effect(fx: &Fixture, effect: &AgentRunEffect) -> AgentRunEntityReply {
    let outcome = fx.dispatcher.answer(effect).await;
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: effect
                .result_operation_id(&common::run_scope())
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
    .expect("the result applies")
}

/// Answers every dispatched effect — here, the turn-one model call.
async fn drive(fx: &Fixture) {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("answer the dispatched effects");
}

/// Settles the run so its committed effects are dispatchable.
async fn settle(fx: &Fixture) {
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    run.settle_side_effects(&fx.router, fx.now())
        .await
        .expect("settle");
}

/// Issue #125: a memory promotion's receipt does not settle a claim append.
/// Delivered to the in-flight append, it is refused `effect-outcome-mismatch`
/// before anything changes: the append stays outstanding, no promotion
/// receipt lands on the loop, and the run's wait does not move. Before the
/// guard the append settled `Succeeded` with a promotion receipt beside it.
#[tokio::test]
async fn a_foreign_outcome_does_not_settle_a_claim_append() {
    let fx = scripted_goal_fixture(RecordingClaimAppendExecutor::new()).await;
    settle(&fx).await;
    apply_append(&fx, "append-foreign", append_request("space-alpha"))
        .await
        .expect("the append applies");
    settle(&fx).await;
    let append = dispatched_effect(&fx, AgentRunEffectKind::ClaimAppendCall).await;
    let resting = resting_at(&fx).await;

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let refusal = run
        .apply(
            AgentRunEntityCommand::RecordEffectResult {
                operation_id: append
                    .result_operation_id(&common::run_scope())
                    .expect("the result operation id derives"),
                effect_id: append.effect_id.clone(),
                generation: append.generation,
                attempt: append.attempts.saturating_add(1),
                fence: 0,
                outcome: Box::new(AgentRunEffectOutcome::MemoryPromotion {
                    promoted: Vec::new(),
                }),
            },
            &fx.router,
            fx.now(),
        )
        .await
        .expect_err("a promotion's receipt settles no claim append");
    assert_eq!(refusal.code(), "effect-outcome-mismatch", "{refusal}");
    drop(run);

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("recover");
    let state = run.state().expect("state");
    let held = committed_append(state)
        .into_iter()
        .find(|held| held.effect_id == append.effect_id)
        .expect("the append is held");
    assert_eq!(held, append, "the append is unmoved");
    assert!(
        state
            .loop_state()
            .expect("the loop is started")
            .memory_promotions()
            .is_empty(),
        "no promotion receipt landed"
    );
    assert_eq!(resting_at(&fx).await, resting, "the run's wait is unmoved");
}

/// A goal world whose first turn calls a tool that answers, cranked to that
/// tool's wait with an append committed beside it, both dispatched: the
/// exact state a sweep that claims into live runs produces. Returns the tool
/// effect and the append effect.
async fn mid_turn_appending_world() -> (
    Fixture,
    Arc<RecordingClaimAppendExecutor>,
    AgentRunEffect,
    AgentRunEffect,
) {
    let executor = RecordingClaimAppendExecutor::new();
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, common::proposing_turn()),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    )
    .with_claim_append_executor(executor.clone());
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
    settle(&fx).await;
    drive(&fx).await;
    settle(&fx).await;
    let (phase, _, turn, _) = resting_at(&fx).await;
    assert_eq!(
        (phase, turn),
        (rakka_agent::AgentLoopPhase::AwaitingTools, 1),
        "the tool is in flight"
    );

    let reply = apply_append(&fx, "mid-turn", append_request("space-alpha"))
        .await
        .expect("the append applies to the live run");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    settle(&fx).await;
    let tool = dispatched_effect(&fx, AgentRunEffectKind::ToolCall).await;
    let append = dispatched_effect(&fx, AgentRunEffectKind::ClaimAppendCall).await;
    (fx, executor, tool, append)
}

/// The executor's answer to the append, applied against the effect record.
async fn answer_append(fx: &Fixture, append: &AgentRunEffect) -> AgentRunEffect {
    let rakka_agent::AgentRunEffectRequest::ClaimAppend {
        append: request,
        provenance,
    } = &append.request
    else {
        panic!("the effect carries the append request");
    };
    let outcome = fx
        .dispatcher
        .claim_append_outcome(&common::run_scope(), append, request, provenance, fx.now())
        .await;
    resolve_claim_append(fx, outcome).await
}

/// Proof 3, the claim half of the wedge: an append committed while the
/// turn's tool is in flight, and the tool result lands first. The turn rests
/// on the turn's own effects, so the run goes on to its next model call with
/// the append still outstanding; the append then resolves on its own and the
/// run completes.
#[tokio::test]
async fn a_claim_append_outstanding_when_the_tool_result_lands_does_not_hold_the_turn_open() {
    let (fx, executor, tool, append) = mid_turn_appending_world().await;

    let reply = answer_effect(&fx, &tool).await;
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    let (phase, status, turn, outstanding) = resting_at(&fx).await;
    assert_eq!(
        (phase, turn),
        (rakka_agent::AgentLoopPhase::AwaitingModel, 2),
        "the tool result rested the turn and the run went on to its next model call \
         (status {status:?}, {outstanding} effect(s) outstanding)"
    );

    let resolved = answer_append(&fx, &append).await;
    assert_eq!(resolved.status, AgentRunEffectStatus::Succeeded);
    assert_eq!(
        executor.invocations().len(),
        1,
        "the append reached the store once"
    );
    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

/// Proof 3, the control: the append resolves first. Its outcome rests
/// nothing — the turn is still waiting on its tool — and the tool result
/// then rests the turn exactly as it always did.
#[tokio::test]
async fn a_claim_append_resolving_before_the_tool_result_rests_nothing() {
    let (fx, executor, tool, append) = mid_turn_appending_world().await;

    let resolved = answer_append(&fx, &append).await;
    assert_eq!(resolved.status, AgentRunEffectStatus::Succeeded);
    assert_eq!(executor.invocations().len(), 1);
    let (phase, _, turn, outstanding) = resting_at(&fx).await;
    assert_eq!(
        (phase, turn, outstanding),
        (rakka_agent::AgentLoopPhase::AwaitingTools, 1, 1),
        "the append's outcome moved nothing: the turn still waits on its tool"
    );

    answer_effect(&fx, &tool).await;
    let (phase, _, turn, _) = resting_at(&fx).await;
    assert_eq!(
        (phase, turn),
        (rakka_agent::AgentLoopPhase::AwaitingModel, 2)
    );
    fx.pump().await.expect("the loop runs to completion");
    let run = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

// ===========================================================================
// A gated claim append opens its gate at the command, outside the turn
// (issue #126).
// ===========================================================================

/// Where the run waits: its phase, status and turn, whatever effects it
/// holds outstanding.
async fn wait_of(fx: &Fixture) -> (rakka_agent::AgentLoopPhase, AgentRunStatus, u64) {
    let (phase, status, turn, _) = resting_at(fx).await;
    (phase, status, turn)
}

/// A deployment that gates claim appends: the `AppendClaim` command opens the
/// gate its spec configured in the command's own transition, on a live run
/// and on an ended one: an `Approval` for a checkpoint, a
/// `SecurityAuthorization` for an authorization. The gate moves no status,
/// phase or terminal record, and a grant dispatches the append and lands the
/// claim.
#[tokio::test]
async fn a_gated_claim_append_by_command_opens_its_gate() {
    let gates = [
        (
            rakka_agent::AgentEffectSpec::idempotent(3)
                .expect("the spec is valid")
                .with_checkpoint_required(),
            AgentCheckpointKind::Approval,
        ),
        (
            rakka_agent::AgentEffectSpec::idempotent(3)
                .expect("the spec is valid")
                .with_authorization_required(),
            AgentCheckpointKind::SecurityAuthorization,
        ),
    ];
    for ended in [false, true] {
        for (spec, kind) in gates.clone() {
            let at = format!("ended: {ended}, {kind:?}");
            let executor = RecordingClaimAppendExecutor::new();
            let mut fx = if ended {
                ended_goal_world(Ending::Completed, executor.clone()).await
            } else {
                let fx = scripted_goal_fixture(executor.clone()).await;
                settle(&fx).await;
                fx
            };
            fx.policies = rakka_agent::AgentEffectPolicies::new()
                .with_claim_append_spec(spec)
                .expect("the policies are valid");
            let before = fx.run_snapshot().await.expect("the run exists");
            let resting = wait_of(&fx).await;

            let reply = apply_append(&fx, "gated", append_request("space-alpha"))
                .await
                .unwrap_or_else(|error| panic!("{at}: the append applies: {error}"));
            assert!(matches!(reply, AgentRunEntityReply::Applied { .. }), "{at}");
            common::assert_gate_invariant(&fx).await;
            let append = {
                let mut run = fx.run();
                run.recover(fx.now()).await.expect("recover");
                let appends = committed_append(run.state().expect("state"));
                assert_eq!(appends.len(), 1, "{at}: one append");
                appends.into_iter().next().expect("one append")
            };
            assert_eq!(append.status, AgentRunEffectStatus::Pending, "{at}");
            let open = {
                let mut run = fx.run();
                run.recover(fx.now()).await.expect("recover");
                run.state()
                    .expect("state")
                    .loop_state()
                    .expect("the loop is started")
                    .open_checkpoints()
                    .to_vec()
            };
            assert_eq!(open.len(), 1, "{at}: the command opened one gate");
            let gate: &AgentCheckpoint = &open[0];
            assert_eq!(gate.kind, kind, "{at}");
            assert_eq!(gate.bound_effect.effect_id, append.effect_id, "{at}");
            assert_eq!(gate.bound_effect.generation, append.generation, "{at}");
            assert_eq!(wait_of(&fx).await, resting, "{at}: the command");

            let mut run = fx.run();
            run.recover(fx.now()).await.expect("recover");
            let reply = run
                .apply(
                    AgentRunEntityCommand::ResolveCheckpoint {
                        operation_id: AgentOperationId::for_agent(
                            AgentOperationKind::CheckpointResolution,
                            &common::agent_scope(),
                            "grant",
                        )
                        .expect("the decision key derives"),
                        checkpoint_id: gate.checkpoint_id.clone(),
                        resolver: PrincipalRef {
                            principal_type: "service".to_string(),
                            principal_id: "claims-reviewer".to_string(),
                            display_name: None,
                        },
                        decision: Box::new(rakka_agent::AgentCheckpointDecision::Approval(
                            rakka_agent::AgentApprovalDecision::Approve {
                                credential_binding: None,
                                expires_at: AgentTimestampMillis::new(u64::MAX / 2),
                                allowed_use_count: 1,
                            },
                        )),
                        telemetry: rakka_agent_workflow::AgentTelemetryContext::default(),
                    },
                    &fx.router,
                    fx.now(),
                )
                .await
                .unwrap_or_else(|error| panic!("{at}: the grant applies: {error}"));
            drop(run);
            assert!(matches!(reply, AgentRunEntityReply::Applied { .. }), "{at}");
            common::assert_gate_invariant(&fx).await;
            assert_eq!(wait_of(&fx).await, resting, "{at}: the grant");

            if ended {
                fx.pump_post_terminal(AgentRunEffectKind::ClaimAppendCall)
                    .await
                    .unwrap_or_else(|error| panic!("{at}: {error}"));
            } else {
                let resolved = answer_append(&fx, &append).await;
                assert_eq!(resolved.status, AgentRunEffectStatus::Succeeded, "{at}");
            }
            common::assert_gate_invariant(&fx).await;
            assert_eq!(executor.invocations().len(), 1, "{at}: the claim landed");
            let mut run = fx.run();
            run.recover(fx.now()).await.expect("recover");
            let appends = committed_append(run.state().expect("state"));
            assert_eq!(
                appends[0].status,
                AgentRunEffectStatus::Succeeded,
                "{at}: the append succeeded"
            );
            drop(run);
            assert_eq!(wait_of(&fx).await, resting, "{at}: the receipt");
            let after = fx.run_snapshot().await.expect("the run exists");
            assert_eq!(after.status, before.status, "{at}: status");
            assert_eq!(
                after.terminal_reason, before.terminal_reason,
                "{at}: reason"
            );
            assert_eq!(after.terminal_at, before.terminal_at, "{at}: stamp");
        }
    }
}

/// The claim half of the cancel rule (issue #126): a `Cancel` with a gated
/// claim append's gate open settles the append `Cancelled` with its gate, in
/// the cancel's own transition. The gate is cancelled, the append never
/// dispatched, and no claim lands. Once turn one's model call settles, the
/// run ends `Cancelled`.
#[tokio::test]
async fn a_cancel_settles_a_gated_claim_append_with_its_gate() {
    let gates = [
        (
            rakka_agent::AgentEffectSpec::idempotent(3)
                .expect("the spec is valid")
                .with_checkpoint_required(),
            AgentCheckpointKind::Approval,
        ),
        (
            rakka_agent::AgentEffectSpec::idempotent(3)
                .expect("the spec is valid")
                .with_authorization_required(),
            AgentCheckpointKind::SecurityAuthorization,
        ),
    ];
    for (spec, kind) in gates {
        let at = format!("{kind:?}");
        let executor = RecordingClaimAppendExecutor::new();
        let mut fx = scripted_goal_fixture(executor.clone()).await;
        settle(&fx).await;
        fx.policies = rakka_agent::AgentEffectPolicies::new()
            .with_claim_append_spec(spec)
            .expect("the policies are valid");

        let reply = apply_append(&fx, "gated", append_request("space-alpha"))
            .await
            .unwrap_or_else(|error| panic!("{at}: the append applies: {error}"));
        assert!(matches!(reply, AgentRunEntityReply::Applied { .. }), "{at}");
        common::assert_gate_invariant(&fx).await;
        let gate = {
            let mut run = fx.run();
            run.recover(fx.now()).await.expect("recover");
            let open = run
                .state()
                .expect("state")
                .loop_state()
                .expect("the loop is started")
                .open_checkpoints()
                .to_vec();
            assert_eq!(open.len(), 1, "{at}: the command opened one gate");
            open.into_iter().next().expect("one gate")
        };
        assert_eq!(gate.kind, kind, "{at}");

        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        let reply = run
            .apply(
                AgentRunEntityCommand::Cancel {
                    operation_id: AgentOperationId::new(
                        AgentOperationKind::Cancellation,
                        [common::TENANT, common::AGENT, "1"],
                    )
                    .expect("the operation id derives"),
                    reason: "operator stopped the work".to_string(),
                },
                &fx.router,
                fx.now(),
            )
            .await
            .unwrap_or_else(|error| panic!("{at}: the cancel applies: {error}"));
        drop(run);
        assert!(matches!(reply, AgentRunEntityReply::Applied { .. }), "{at}");
        common::assert_gate_invariant(&fx).await;

        let mut run = fx.run();
        run.recover(fx.now()).await.expect("recover");
        let state = run.state().expect("state");
        let closed = state
            .loop_state()
            .expect("the loop is started")
            .open_checkpoints()
            .iter()
            .find(|held| held.checkpoint_id == gate.checkpoint_id)
            .map(|held| held.status);
        assert_eq!(
            closed,
            Some(rakka_agent::AgentCheckpointStatus::Cancelled),
            "{at}: the gate is cancelled"
        );
        let appends = committed_append(state);
        assert_eq!(appends.len(), 1, "{at}: one append");
        assert_eq!(
            appends[0].status,
            AgentRunEffectStatus::Cancelled,
            "{at}: the append is settled with its gate"
        );
        assert_eq!(appends[0].attempts, 0, "{at}: it never dispatched");
        drop(run);
        assert_eq!(
            fx.run_snapshot().await.expect("the run exists").status,
            AgentRunStatus::Cancelling,
            "{at}: the model call in flight still holds the run"
        );

        drive(&fx).await;
        common::assert_gate_invariant(&fx).await;
        assert_eq!(
            fx.run_snapshot().await.expect("the run exists").status,
            AgentRunStatus::Cancelled,
            "{at}: nothing holds the run once the model call settles"
        );
        assert!(executor.invocations().is_empty(), "{at}: no claim landed");
    }
}
