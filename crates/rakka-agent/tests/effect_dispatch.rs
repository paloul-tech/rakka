//! The effect machine over the real dispatch pipeline, with dispatcher loss
//! injected at every durable boundary.
//!
//! Specification: sections 11.3 through 11.6; scenarios 5 through 9 of
//! section 18, and the dispatch invariants of 11.4. The run's effects travel
//! the production path here: committed into the run's durable state, flushed
//! as dispatch tickets into the agent-workflow outbox, leased and fenced by
//! the dispatcher fleet, invoked through the model adapter and tool executor,
//! and returned as durable result commands.
//!
//! "Dispatcher loss" is literal: the [`KillSwitchProbe`] aborts the worker
//! between two durable writes, the worker is dropped, the clock advances past
//! its lease, and a *new* worker recovers from durable state alone. The
//! [`RecordingToolExecutor`]'s invocation log is the external system: how many
//! times it committed, and under which idempotency key, is what each safety
//! class makes promises about.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::{
    DeterministicModelAdapter, InProcessRunResultDelivery, KillSwitchProbe, RecordingToolExecutor,
    ScriptedCredentialResolver, ScriptedDispatcher, ScriptedReconciler, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentCredentialBindingRef, AgentDispatchWindow, AgentEffectPolicies, AgentEffectResolution,
    AgentEffectSpec, AgentLoopPhase, AgentModelTurn, AgentReconciliationFinding,
    AgentReconciliationProtocolRef, AgentRunEffectDispatcher, AgentRunEffectOutcome,
    AgentRunEffectStatus, AgentRunEntityCommand, AgentRunStatus, AgentRunTerminalReason,
    AgentTaskContent, AgentTaskStatus, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    WorkflowAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::WorkflowState;
use rakka_agent_workflow::{
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherWorkerId,
};
use rakka_persistence::InMemoryDurableStateStore;

mod common;

use common::*;

type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;
type FleetStore = InMemoryDurableStateStore<AgentDispatcherFleetState>;
type WorkflowSink = WorkflowAgentRunEffectSink<WorkflowStore, SharedAtomicWorkflowClock>;
type Pipeline =
    AgentRunEffectDispatcher<WorkflowStore, FleetStore, RunStore, SharedAtomicWorkflowClock>;

const LEASE_MS: u64 = 60_000;
const TOOL: &str = "charge-card";

fn tool_calling_turn(tool: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me do that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                AgentToolId::new(tool).expect("tool id should be valid"),
                serde_json::json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
}

/// The dispatch fixture: the common task-and-run fixture over the *durable*
/// workflow-outbox sink, plus the fleet, the executors, and the probe.
///
/// A "worker" is deliberately transient: [`DispatchFixture::pipeline`] builds
/// a fresh one from the shared durable stores, which is exactly what recovery
/// after a dispatcher death looks like.
struct DispatchFixture {
    fx: Fixture<DeterministicModelAdapter, WorkflowSink>,
    adapter: DeterministicModelAdapter,
    workflow_store: WorkflowStore,
    fleet_store: FleetStore,
    wf_clock: SharedAtomicWorkflowClock,
    tools: RecordingToolExecutor,
    reconciler: ScriptedReconciler,
    credentials: ScriptedCredentialResolver,
    probe: KillSwitchProbe,
}

impl DispatchFixture {
    fn new(
        adapter: DeterministicModelAdapter,
        policies: AgentEffectPolicies,
        tools: RecordingToolExecutor,
        reconciler: ScriptedReconciler,
    ) -> Self {
        let counter = Arc::new(AtomicU64::new(1));
        let wf_clock = SharedAtomicWorkflowClock::new(counter.clone());
        let workflow_store = WorkflowStore::new();
        let fleet_store = FleetStore::new();
        let sink = WorkflowAgentRunEffectSink::new(workflow_store.clone(), wf_clock.clone());
        let fx = Fixture::with_sink(
            ScriptedDispatcher::with_adapter(adapter.clone()),
            sink,
            policies,
            counter,
        );
        Self {
            fx,
            adapter,
            workflow_store,
            fleet_store,
            wf_clock,
            tools,
            reconciler,
            credentials: ScriptedCredentialResolver::new("live-secret-token"),
            probe: KillSwitchProbe::new(),
        }
    }

    async fn start(&self) {
        self.fx.instantiate_agent().await;
        self.fx.create_task().await;
    }

    /// A fresh dispatch worker over the shared durable stores.
    fn pipeline(&self) -> Pipeline {
        AgentRunEffectDispatcher::new(
            AgentDispatcherWorkerId::new("worker-1"),
            self.workflow_store.clone(),
            self.fleet_store.clone(),
            self.fx.runs.clone(),
            self.wf_clock.clone(),
            Arc::new(self.adapter.clone()),
            Arc::new(self.tools.clone()),
            Arc::new(
                InProcessRunResultDelivery::new(
                    self.fx.runs.clone(),
                    self.fx.effects.clone(),
                    self.fx.router.clone(),
                    self.fx.clock.clone(),
                )
                .with_effect_policies(self.fx.policies.clone()),
            ),
        )
        .with_fleet_settings(AgentDispatcherFleetSettings::new(16, LEASE_MS))
        .with_probe(Arc::new(self.probe.clone()))
        .with_reconciler(Arc::new(self.reconciler.clone()))
        .with_credential_resolver(Arc::new(self.credentials.clone()))
    }

    fn expire_lease(&self) {
        self.wf_clock.advance(LEASE_MS + 1);
    }

    /// Settles the entities from durable state: the task drives its owed
    /// exchanges, the run cranks its loop and flushes its tickets.
    async fn settle(&self) {
        let now = self.fx.now();
        let mut task = rakka_agent::AgentTaskEntityStore::new(
            task_scope(),
            self.fx.tasks.clone(),
            self.fx.agents.clone(),
            self.fx.history.clone(),
        );
        task.recover(now).await.expect("the task recovers");
        task.settle_side_effects(&self.fx.router, now)
            .await
            .expect("the task settles");

        let now = self.fx.now();
        let mut run = self.fx.run();
        run.recover(now).await.expect("the run recovers");
        run.settle_side_effects(&self.fx.router, now)
            .await
            .expect("the run settles");
    }

    /// Drives entities and the dispatch pipeline until the run is terminal or
    /// nothing moves. Each round uses a fresh worker.
    async fn pump(&self) {
        for _round in 0..16 {
            self.settle().await;
            let pass = self
                .pipeline()
                .pump_run(&run_scope())
                .await
                .expect("the dispatch pass runs");
            let snapshot = self.fx.run_snapshot().await;
            let terminal = snapshot
                .as_ref()
                .is_some_and(|run| run.status.is_terminal());
            if terminal {
                return;
            }
            if pass.registered == 0
                && pass.claimed == 0
                && pass.delivered == 0
                && pass.cancelled == 0
            {
                return;
            }
        }
        panic!("the dispatch pump did not quiesce");
    }

    /// The run-side status of the first effect of the given turn slot.
    async fn effect_status(&self, slot: usize) -> Option<AgentRunEffectStatus> {
        let state = rakka_agent::load_agent_run_state(
            &self.fx.runs,
            &run_scope(),
            &rakka_agent::AgentSchemaPolicy::default(),
        )
        .await
        .expect("the run state loads")?;
        state
            .loop_state()?
            .effects()
            .iter()
            .find(|effect| effect.slot == slot)
            .map(|effect| effect.status)
    }
}

fn tool_policies(spec: AgentEffectSpec) -> AgentEffectPolicies {
    AgentEffectPolicies::new()
        .with_tool_spec(
            AgentToolId::new(TOOL).expect("tool id should be valid"),
            spec,
        )
        .expect("the tool spec is valid")
}

// ---------------------------------------------------------------------------
// Scenario 5: dispatcher loss before durable `Started` safely redispatches.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatcher_loss_before_started_safely_redispatches() {
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new().with_turn(proposing_turn("resolved")),
        AgentEffectPolicies::default(),
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.start().await;
    fx.settle().await;

    // The worker claims the model ticket and dies before writing `Started`.
    fx.probe.arm(AgentDispatchWindow::BeforeStarted);
    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert!(pass.died, "the probe killed the worker");
    assert_eq!(
        fx.adapter.calls(),
        0,
        "nothing was invoked before durable Started"
    );

    // A new worker recovers after the lease expires. The outbox row is still
    // scheduled — proof the target was never invoked — so redispatch is safe
    // for every class, and exactly one invocation happens.
    fx.expire_lease();
    fx.pump().await;

    assert_eq!(fx.adapter.calls(), 1, "exactly one model invocation");
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(
        fx.fx.task_snapshot().await.status,
        AgentTaskStatus::Completed
    );
}

// ---------------------------------------------------------------------------
// Scenario 6: dispatcher loss after `Started` retries a read-only effect
// under policy.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatcher_loss_after_started_retries_a_read_only_effect_under_policy() {
    let policies = AgentEffectPolicies::new()
        .with_model_spec(
            AgentEffectSpec::read_only()
                .with_max_attempts(2)
                .expect("the spec is valid"),
        )
        .expect("the policies are valid");
    // The adapter must declare at least what the intent's policy uses: the
    // declaration is the permitted ceiling, re-enforced at dispatch.
    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, proposing_turn("resolved"))
        .with_retry_policy(
            rakka_agent::AgentModelRetryPolicy::read_only(2).expect("the policy is valid"),
        )
        .expect("the adapter accepts the policy");
    let fx = DispatchFixture::new(
        adapter,
        policies,
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.start().await;
    fx.settle().await;

    // The worker writes durable `Started`, then dies before invoking.
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert!(pass.died);
    assert_eq!(fx.adapter.calls(), 0, "the invocation never happened");

    // Recovery finds the ambiguous window — the row says `Dispatching` — and
    // the read-only class permits a bounded retry: one attempt is burned for
    // the loss, and the second (and last) attempt invokes.
    fx.expire_lease();
    fx.pump().await;

    assert_eq!(
        fx.adapter.calls(),
        1,
        "one retry, under the two-attempt bound"
    );
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

#[tokio::test]
async fn a_read_only_effect_whose_policy_permits_one_attempt_exhausts_instead_of_retrying() {
    // Same loss, but the default policy: one attempt, no automatic retry. The
    // ambiguous loss consumes the only attempt, so the generation exhausts and
    // the run stops with a structured reason instead of silently re-billing.
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new().with_turn(proposing_turn("resolved")),
        AgentEffectPolicies::default(),
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.start().await;
    fx.settle().await;

    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert!(pass.died);

    fx.expire_lease();
    fx.pump().await;

    assert_eq!(fx.adapter.calls(), 0, "no attempt remained to retry with");
    assert_eq!(
        fx.effect_status(0).await,
        Some(AgentRunEffectStatus::Exhausted)
    );
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    assert!(matches!(
        run.terminal_reason,
        Some(AgentRunTerminalReason::EffectFailed { .. })
    ));
}

// ---------------------------------------------------------------------------
// Scenario 7: dispatcher loss after `Started` reuses the same idempotency key
// for an idempotent effect.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatcher_loss_after_started_reuses_the_idempotency_key_of_an_idempotent_effect() {
    let policies = tool_policies(AgentEffectSpec::idempotent(3).expect("the spec is valid"));
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        policies,
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.start().await;

    // Turn one: the model asks for the tool; the tool ticket reaches the
    // outbox. The worker invokes the tool — the external system commits — and
    // dies before any receipt is recorded.
    fx.pump_until_tool_ticket().await;
    fx.probe.arm(AgentDispatchWindow::AfterInvocation);
    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert!(pass.died);
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "the target committed once"
    );

    // Recovery retries — that is what the idempotent class buys — and the
    // second invocation hands the target the *same* external idempotency key,
    // which is what makes the duplicate safe.
    fx.expire_lease();
    fx.pump().await;

    let invocations = fx.tools.invocations();
    assert_eq!(invocations.len(), 2, "the retry re-invoked the target");
    assert_eq!(
        invocations[0].idempotency_key, invocations[1].idempotency_key,
        "the retry reused the generation's external idempotency key"
    );
    assert_eq!(invocations[0].generation, invocations[1].generation);
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

// ---------------------------------------------------------------------------
// Scenario 8: dispatcher loss after `Started` reconciles a reconcileable
// effect before any retry.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatcher_loss_after_started_reconciles_before_any_retry() {
    // The protocol proves the invocation happened and returns its outcome, so
    // the target is never touched again.
    let protocol = AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid");
    let policies =
        tool_policies(AgentEffectSpec::reconcileable(protocol, 3).expect("the spec is valid"));
    let established = AgentTaskContent::inline(serde_json::json!({ "receipt": "r-77" }))
        .expect("the content is inline-bounded");
    let reconciler = ScriptedReconciler::new().with_finding(AgentReconciliationFinding::Executed {
        outcome: Box::new(AgentRunEffectOutcome::Tool {
            call_id: AgentToolCallId::new("call-1").expect("call id should be valid"),
            content: established.clone(),
        }),
    });
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        policies,
        RecordingToolExecutor::new(),
        reconciler,
    );
    fx.start().await;

    fx.pump_until_tool_ticket().await;
    fx.probe.arm(AgentDispatchWindow::AfterInvocation);
    let _pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert_eq!(fx.tools.invocation_count(TOOL), 1);

    fx.expire_lease();
    fx.pump().await;

    // Reconciled, not retried: the protocol was queried, the target was not.
    assert_eq!(fx.reconciler.queries(), 1, "the protocol was queried once");
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "the authoritative outcome was recorded without re-invocation"
    );
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

#[tokio::test]
async fn a_reconcileable_effect_proven_absent_is_retried() {
    // The loss happens before the invocation, and the protocol proves it: the
    // retry is a fresh invocation, allowed exactly because absence was proven.
    let protocol = AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid");
    let policies =
        tool_policies(AgentEffectSpec::reconcileable(protocol, 3).expect("the spec is valid"));
    let reconciler =
        ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted);
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        policies,
        RecordingToolExecutor::new(),
        reconciler,
    );
    fx.start().await;

    fx.pump_until_tool_ticket().await;
    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    let _pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        0,
        "death before invocation"
    );

    fx.expire_lease();
    fx.pump().await;

    assert_eq!(fx.reconciler.queries(), 1);
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "proven absent, then invoked exactly once"
    );
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

#[tokio::test]
async fn an_unknown_reconciliation_finding_is_requeried_before_any_retry() {
    // The tool commits externally and the worker dies without a receipt, but
    // the protocol cannot establish the outcome yet — the ledger has not
    // ingested the attempt — and answers `Unknown`. Burning that attempt
    // rewrites the outbox row to a retryable failure, which must not launder
    // the ambiguity into a routine retry: the next claim queries the protocol
    // *again*, and only its answer decides. Here the second query proves the
    // invocation happened, so the target is never touched twice.
    let protocol = AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid");
    let policies =
        tool_policies(AgentEffectSpec::reconcileable(protocol, 3).expect("the spec is valid"));
    let reconciler = ScriptedReconciler::new()
        .with_finding(AgentReconciliationFinding::Unknown)
        .with_finding(AgentReconciliationFinding::Executed {
            outcome: Box::new(AgentRunEffectOutcome::Tool {
                call_id: AgentToolCallId::new("call-1").expect("call id should be valid"),
                content: AgentTaskContent::inline(serde_json::json!({ "receipt": "r-81" }))
                    .expect("the content is inline-bounded"),
            }),
        });
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        policies,
        RecordingToolExecutor::new(),
        reconciler,
    );
    fx.start().await;

    fx.pump_until_tool_ticket().await;
    fx.probe.arm(AgentDispatchWindow::AfterInvocation);
    let _pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert_eq!(fx.tools.invocation_count(TOOL), 1, "the target committed");

    fx.expire_lease();
    fx.pump().await;

    // Queried twice — the `Unknown` answer deferred the decision, it never
    // authorized anything — and the retry-scheduled row went back to the
    // protocol instead of the target.
    assert_eq!(
        fx.reconciler.queries(),
        2,
        "the retry-scheduled row was reconciled again, not redispatched"
    );
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "an attempt never proven absent is never re-invoked"
    );
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

// ---------------------------------------------------------------------------
// Scenario 9: dispatcher loss in the ambiguous non-idempotent window produces
// exactly one durable Indeterminate outcome and no automatic re-invocation.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_ambiguous_non_idempotent_loss_parks_exactly_one_indeterminate_outcome() {
    // The default tool policy *is* non-idempotent: an unclassified tool fails
    // safe ([specification 11.2]).
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        AgentEffectPolicies::default(),
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.start().await;

    fx.pump_until_tool_ticket().await;
    fx.probe.arm(AgentDispatchWindow::AfterInvocation);
    let _pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert_eq!(fx.tools.invocation_count(TOOL), 1, "the target committed");

    // Recovery parks the generation. Running recovery *twice* — a second
    // worker racing the first — must not change anything: the result operation
    // id and the run's own effect fence make the Indeterminate word single.
    fx.expire_lease();
    fx.pump().await;
    fx.expire_lease();
    fx.pump().await;

    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "no automatic re-invocation, ever"
    );
    assert_eq!(
        fx.effect_status(1).await,
        Some(AgentRunEffectStatus::Indeterminate),
        "the tool effect of slot 1 parks; the model effect of slot 0 already succeeded"
    );
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        AgentRunStatus::WaitingForReconciliation,
        "the run parks for the explicit decision"
    );
    assert_eq!(run.phase, AgentLoopPhase::AwaitingTools);
    assert!(
        run.terminal_reason.is_none(),
        "parking for reconciliation is not a failure"
    );
}

#[tokio::test]
async fn a_reconciliation_decision_resumes_the_run_and_not_executed_mints_a_new_generation() {
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        AgentEffectPolicies::default(),
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.probe.arm(AgentDispatchWindow::AfterInvocation);
    let _pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    fx.expire_lease();
    fx.pump().await;

    let (effect_id, generation) = fx.parked_effect().await;

    // An operator proves the invocation never happened. A new generation is
    // authorized: a fresh dispatch ticket, a fresh attempt budget — and the
    // target is invoked again, as a genuinely new invocation.
    let mut run = fx.fx.run();
    let now = fx.fx.now();
    run.recover(now).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::ResolveIndeterminateEffect {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::CheckpointResolution,
                [TENANT, AGENT, "resolve-1"],
            )
            .expect("the operation id derives"),
            effect_id: effect_id.clone(),
            generation,
            resolution: Box::new(AgentEffectResolution::ConfirmedNotExecuted),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("the resolution applies");

    fx.pump().await;

    let invocations = fx.tools.invocations();
    assert_eq!(
        invocations.len(),
        2,
        "the new generation invoked the target"
    );
    assert_eq!(invocations[0].generation, 1);
    assert_eq!(invocations[1].generation, 2);
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(
        fx.fx.task_snapshot().await.status,
        AgentTaskStatus::Completed
    );
}

// ---------------------------------------------------------------------------
// Scenario 10 (generation half): a result for a superseded generation is
// refused by the run's own fence.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_result_for_a_superseded_generation_is_refused() {
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        AgentEffectPolicies::default(),
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.probe.arm(AgentDispatchWindow::AfterInvocation);
    let _pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    fx.expire_lease();
    fx.pump().await;

    let (effect_id, generation) = fx.parked_effect().await;
    let mut run = fx.fx.run();
    let now = fx.fx.now();
    run.recover(now).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::ResolveIndeterminateEffect {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::CheckpointResolution,
                [TENANT, AGENT, "resolve-1"],
            )
            .expect("the operation id derives"),
            effect_id: effect_id.clone(),
            generation,
            resolution: Box::new(AgentEffectResolution::ConfirmedNotExecuted),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("the resolution applies");

    // A late result from the superseded generation arrives — the lost worker's
    // answer finally surfacing. The run holds generation 2 now, so the fence
    // refuses it: the reconciliation happened precisely because that attempt's
    // outcome could not be trusted.
    let stale = run
        .apply(
            AgentRunEntityCommand::RecordEffectResult {
                operation_id: rakka_agent::AgentOperationId::new(
                    rakka_agent::AgentOperationKind::Command,
                    [TENANT, AGENT, "stale-generation"],
                )
                .expect("the operation id derives"),
                effect_id,
                generation,
                attempt: 1,
                fence: 1,
                outcome: Box::new(AgentRunEffectOutcome::Tool {
                    call_id: AgentToolCallId::new("call-1").expect("call id should be valid"),
                    content: AgentTaskContent::inline(serde_json::json!({ "late": true }))
                        .expect("the content is inline-bounded"),
                }),
            },
            &fx.fx.router,
            fx.fx.now(),
        )
        .await;
    assert_eq!(
        stale
            .expect_err("a superseded generation's result must be refused")
            .code(),
        "run-stale-effect-generation"
    );
}

// ---------------------------------------------------------------------------
// Scenario 57, effect half: cancellation with an ambiguous consequential
// effect fences all new work, remains nonterminal in reconciliation, and
// projects terminal cancellation only after the outcome is resolved.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancellation_with_an_ambiguous_consequential_effect_stays_in_reconciliation() {
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        AgentEffectPolicies::default(),
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;

    // The non-idempotent tool commits externally; the worker dies without a
    // receipt.
    fx.probe.arm(AgentDispatchWindow::AfterInvocation);
    let _pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert_eq!(fx.tools.invocation_count(TOOL), 1);

    // Cancellation arrives while the attempt is ambiguous.
    let mut run = fx.fx.run();
    let now = fx.fx.now();
    run.recover(now).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::Cancellation,
                [TENANT, AGENT, "1"],
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("cancellation applies");
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Cancelling,
        "the ambiguous effect keeps the run nonterminal"
    );

    // Recovery under the cancellation: the ambiguous non-idempotent effect
    // parks as indeterminate — a cancellation request does not make an unknown
    // outcome known — and the run stays nonterminal in reconciliation.
    fx.expire_lease();
    fx.pump().await;

    let run_snapshot = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run_snapshot.status,
        AgentRunStatus::WaitingForReconciliation
    );
    assert!(
        !run_snapshot.status.is_terminal(),
        "terminal cancellation must not be projected over an unresolved effect"
    );
    assert_eq!(
        run_snapshot
            .terminal_reason
            .as_ref()
            .expect("the cancellation request is recorded")
            .code(),
        "cancellation-requested"
    );
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "all new work is fenced: nothing was dispatched after the cancel"
    );

    // Only the explicit decision resolves the wind-down. The invocation is
    // confirmed to have executed; the run then — and only then — becomes
    // terminally cancelled.
    let (effect_id, generation) = fx.parked_effect().await;
    let mut run = fx.fx.run();
    let now = fx.fx.now();
    run.recover(now).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::ResolveIndeterminateEffect {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::CheckpointResolution,
                [TENANT, AGENT, "resolve-57"],
            )
            .expect("the operation id derives"),
            effect_id,
            generation,
            resolution: Box::new(AgentEffectResolution::ConfirmedExecuted {
                outcome: Box::new(AgentRunEffectOutcome::Tool {
                    call_id: AgentToolCallId::new("call-1").expect("call id should be valid"),
                    content: AgentTaskContent::inline(serde_json::json!({ "receipt": "r-9" }))
                        .expect("the content is inline-bounded"),
                }),
            }),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("the resolution applies");

    let run_snapshot = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run_snapshot.status, AgentRunStatus::Cancelled);
    assert_eq!(run_snapshot.phase, AgentLoopPhase::Complete);
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "resolution recorded the outcome without re-invoking"
    );
}

// ---------------------------------------------------------------------------
// Dispatch-time credential resolution only.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn credentials_are_resolved_at_dispatch_only_and_never_persisted() {
    let policies = tool_policies(
        AgentEffectSpec::idempotent(2)
            .expect("the spec is valid")
            .with_credential_binding(
                AgentCredentialBindingRef::new("payments-api").expect("the binding is valid"),
            ),
    );
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        policies,
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.start().await;

    // The intent is committed and the ticket is durable — and no credential
    // has been resolved: commit time is not dispatch time.
    fx.pump_until_tool_ticket().await;
    assert_eq!(fx.credentials.resolutions(), 0);

    fx.pump().await;

    // Exactly one resolution, inside the one dispatch attempt, and the
    // executor saw it.
    assert_eq!(fx.credentials.resolutions(), 1);
    let invocations = fx.tools.invocations();
    let tool_invocation = invocations
        .iter()
        .find(|invocation| invocation.tool == TOOL)
        .expect("the tool was invoked");
    assert!(tool_invocation.with_credential);

    // The resolved value appears in no durable record: not the run's state,
    // not the workflow outbox, not the fleet.
    let run_state = rakka_agent::load_agent_run_state(
        &fx.fx.runs,
        &run_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run exists");
    let encoded = serde_json::to_string(&run_state).expect("the state serializes");
    assert!(!encoded.contains("live-secret-token"));

    let mut inbox = rakka_agent_workflow::AgentRunInbox::with_clock(
        rakka_agent::workflow_run_id(&run_scope()),
        fx.workflow_store.clone(),
        fx.wf_clock.clone(),
    );
    let workflow = inbox.recover().await.expect("the workflow state loads");
    let encoded = serde_json::to_string(workflow).expect("the workflow serializes");
    assert!(!encoded.contains("live-secret-token"));
}

// ---------------------------------------------------------------------------
// The adapter's declared retry policy is re-enforced at dispatch.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_effect_policy_weaker_than_the_adapters_declaration_fails_closed_at_dispatch() {
    // The adapter declares its calls non-idempotent (a provider whose ambiguous
    // billing must never be retried); the deployment misconfigures the model
    // spec as retryable read-only. The intent's policy would let recovery retry
    // what the adapter declared unsafe, so dispatch refuses before invoking.
    let adapter = DeterministicModelAdapter::new()
        .with_turn(proposing_turn("resolved"))
        .with_retry_policy(rakka_agent::AgentModelRetryPolicy {
            safety_class: rakka_agent::AgentEffectSafetyClass::NonIdempotent,
            max_attempts: 1,
        })
        .expect("the adapter policy is valid");
    let policies = AgentEffectPolicies::new()
        .with_model_spec(
            AgentEffectSpec::read_only()
                .with_max_attempts(3)
                .expect("the spec is valid"),
        )
        .expect("the policies are valid");
    let fx = DispatchFixture::new(
        adapter,
        policies,
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.adapter.calls(), 0, "nothing was invoked");
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Failed);
    let Some(AgentRunTerminalReason::EffectFailed { code, .. }) = run.terminal_reason else {
        panic!("expected an effect failure, got {:?}", run.terminal_reason);
    };
    assert_eq!(code, "model-policy-conflict");
}

// ---------------------------------------------------------------------------
// The first rough per-turn durable-write measurement (formalized in slice
// 1.14).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn per_turn_durable_write_count_and_latency_measurement() {
    // One clean model turn, end to end over the integrated pipeline: accept
    // the assignment, commit the model effect, ticket it, claim it, invoke,
    // deliver the turn, propose the result, and let the task accept it.
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new().with_turn(proposing_turn("resolved")),
        AgentEffectPolicies::default(),
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    let started = std::time::Instant::now();
    fx.start().await;
    fx.pump().await;
    let elapsed = started.elapsed();

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);

    // Durable writes per store. The run and task stores count compare-and-sets
    // directly; the workflow and fleet stores expose them as their final
    // record revision.
    let run_writes = fx.fx.runs.writes();
    let workflow_revision = {
        use rakka_persistence::DurableStateStore;
        let inbox = rakka_agent_workflow::AgentRunInbox::with_clock(
            rakka_agent::workflow_run_id(&run_scope()),
            fx.workflow_store.clone(),
            fx.wf_clock.clone(),
        );
        let persistence_id = inbox.inner().persistence_id().clone();
        fx.workflow_store
            .load(&persistence_id)
            .await
            .expect("the workflow state loads")
            .map(|record| record.revision.get())
            .unwrap_or(0)
    };
    println!(
        "one full turn (accept -> model effect -> ticket -> claim -> invoke -> deliver -> \
         propose -> accept): run-store writes = {run_writes}, workflow-store revision = \
         {workflow_revision}, wall time = {elapsed:?}"
    );

    // A loose ceiling so a future change that multiplies the per-turn write
    // count cannot land silently; the real budget is set in slice 1.14.
    assert!(
        run_writes <= 16,
        "one turn made {run_writes} run-store writes; the durable-boundary design expects \
         well under 16"
    );
}

impl DispatchFixture {
    /// Drives until the run's first *tool* ticket is durably in the outbox and
    /// nothing else is claimable: the model turn of turn one is answered, and
    /// the tool effect of slot 1 is `Ready`.
    async fn pump_until_tool_ticket(&self) {
        for _round in 0..8 {
            self.settle().await;
            let ready = self.effect_status(1).await == Some(AgentRunEffectStatus::Ready);
            if ready {
                // Flush the ticket to the outbox once more (idempotent), so
                // the pipeline can register it.
                self.settle().await;
                return;
            }
            let _pass = self
                .pipeline()
                .pump_run(&run_scope())
                .await
                .expect("the pass runs");
        }
        panic!("the tool ticket never became ready");
    }

    /// The parked indeterminate effect's identity.
    async fn parked_effect(
        &self,
    ) -> (
        rakka_agent_workflow::AgentEffectId,
        rakka_agent::AgentEffectGeneration,
    ) {
        let state = rakka_agent::load_agent_run_state(
            &self.fx.runs,
            &run_scope(),
            &rakka_agent::AgentSchemaPolicy::default(),
        )
        .await
        .expect("the run state loads")
        .expect("the run exists");
        let effect = state
            .loop_state()
            .expect("the loop exists")
            .indeterminate_effects()
            .next()
            .expect("an effect is parked")
            .clone();
        (effect.effect_id, effect.generation)
    }
}
