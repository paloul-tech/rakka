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
    CrashingStateStore, DeterministicModelAdapter, InProcessRunResultDelivery, KillSwitchProbe,
    RecordingToolExecutor, ScriptedCredentialResolver, ScriptedDispatcher, ScriptedReconciler,
    SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentBudgetCeilings, AgentCredentialBindingRef, AgentDispatchWindow, AgentEffectResolution,
    AgentEffectSpec, AgentEntityAuthority, AgentLoopPhase, AgentModelTurn,
    AgentReconciliationFinding, AgentReconciliationProtocolRef, AgentRunEffectDispatcher,
    AgentRunEffectOutcome, AgentRunEffectStatus, AgentRunEntityCommand, AgentRunStatus,
    AgentRunTerminalReason, AgentTaskContent, AgentTaskStatus, AgentToolAuthority, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, AgentToolRegistry, WorkflowAgentRunEffectSink,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::WorkflowState;
use rakka_agent_workflow::{
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherWorkerId,
};

mod common;

use common::*;

type WorkflowStore = CrashingStateStore<WorkflowState>;
type FleetStore = CrashingStateStore<AgentDispatcherFleetState>;
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
    registry: AgentToolRegistry,
    workflow_store: WorkflowStore,
    fleet_store: FleetStore,
    wf_clock: SharedAtomicWorkflowClock,
    tools: RecordingToolExecutor,
    reconciler: ScriptedReconciler,
    credentials: ScriptedCredentialResolver,
    probe: KillSwitchProbe,
    promotions: Option<Arc<dyn rakka_agent::AgentMemoryPromotionExecutor>>,
    segments: Option<Arc<dyn rakka_agent::AgentSegmentSink>>,
}

impl DispatchFixture {
    fn new(
        adapter: DeterministicModelAdapter,
        registry: AgentToolRegistry,
        model_spec: Option<AgentEffectSpec>,
        tools: RecordingToolExecutor,
        reconciler: ScriptedReconciler,
    ) -> Self {
        // The registry is the single source: the commit-time policies are its
        // projection, and the dispatch-time authority answers from the same
        // bindings.
        let mut policies = registry
            .effect_policies()
            .expect("the registry projects valid policies");
        if let Some(spec) = model_spec {
            policies = policies
                .with_model_spec(spec)
                .expect("the model spec is valid");
        }
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
            registry,
            workflow_store,
            fleet_store,
            wf_clock,
            tools,
            reconciler,
            credentials: ScriptedCredentialResolver::new("live-secret-token"),
            probe: KillSwitchProbe::new(),
            promotions: None,
            segments: None,
        }
    }

    /// Wires the dispatch pipeline with a segment sink, so what the worker
    /// closes is observable.
    fn with_segments(mut self, sink: Arc<dyn rakka_agent::AgentSegmentSink>) -> Self {
        self.segments = Some(sink);
        self
    }

    /// Wires the run with session memory and the pipeline with the promotion
    /// executor over the same stores.
    fn with_promotions(
        mut self,
        memory: rakka_agent::AgentRunMemory,
        executor: Arc<dyn rakka_agent::AgentMemoryPromotionExecutor>,
    ) -> Self {
        self.fx = self.fx.with_memory(memory);
        self.promotions = Some(executor);
        self
    }

    async fn start(&self) {
        self.fx
            .instantiate_agent_with_envelope(envelope_for_registry(&self.registry))
            .await;
        self.fx.create_task().await;
    }

    /// A fresh dispatch worker over the shared durable stores.
    fn pipeline(&self) -> Pipeline {
        let mut pipeline = AgentRunEffectDispatcher::new(
            AgentDispatcherWorkerId::new("worker-1"),
            self.workflow_store.clone(),
            self.fleet_store.clone(),
            self.fx.runs.clone(),
            self.wf_clock.clone(),
            Arc::new(self.adapter.clone()),
            Arc::new(self.tools.clone()),
            Arc::new(AgentEntityAuthority::new(
                self.fx.agents.clone(),
                AgentToolAuthority::new(self.registry.clone()),
            )),
            Arc::new({
                let mut delivery = InProcessRunResultDelivery::new(
                    self.fx.runs.clone(),
                    self.fx.effects.clone(),
                    self.fx.router.clone(),
                    self.fx.clock.clone(),
                )
                .with_effect_policies(self.fx.policies.clone());
                if let Some(metrics) = &self.fx.metrics {
                    delivery = delivery.with_metrics(metrics.clone());
                }
                delivery
            }),
        )
        .with_fleet_settings(AgentDispatcherFleetSettings::new(16, LEASE_MS))
        .with_probe(Arc::new(self.probe.clone()))
        .with_reconciler(Arc::new(self.reconciler.clone()))
        .with_credential_resolver(Arc::new(self.credentials.clone()));
        if let Some(promotions) = &self.promotions {
            pipeline = pipeline.with_memory_promotion_executor(promotions.clone());
        }
        if let Some(segments) = &self.segments {
            pipeline = pipeline.with_segments(segments.clone());
        }
        pipeline
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

    /// [`Self::settle`], but surfacing the first error instead of panicking —
    /// what a sweep needs, because an armed [`CrashingStateStore`] kills the
    /// owner mid-settle and the injected loss is the point, not a failure.
    async fn try_settle(&self) -> Result<(), String> {
        let now = self.fx.now();
        let mut task = rakka_agent::AgentTaskEntityStore::new(
            task_scope(),
            self.fx.tasks.clone(),
            self.fx.agents.clone(),
            self.fx.history.clone(),
        );
        task.recover(now)
            .await
            .map_err(|error| error.code().to_string())?;
        task.settle_side_effects(&self.fx.router, now)
            .await
            .map_err(|error| error.code().to_string())?;

        let now = self.fx.now();
        let mut run = self.fx.run();
        run.recover(now)
            .await
            .map_err(|error| error.code().to_string())?;
        run.settle_side_effects(&self.fx.router, now)
            .await
            .map_err(|error| error.code().to_string())?;
        Ok(())
    }

    /// [`Self::pump`], but surfacing the first error instead of panicking,
    /// under the same sweep contract as [`Self::try_settle`]. Reads only
    /// durable state, so calling it after a crash is the same operation as
    /// calling it after a success.
    async fn try_pump(&self) -> Result<(), String> {
        for _round in 0..16 {
            self.try_settle().await?;
            let pass = self
                .pipeline()
                .pump_run(&run_scope())
                .await
                .map_err(|error| error.code().to_string())?;
            let terminal = {
                let mut run = self.fx.run();
                run.recover(self.fx.now())
                    .await
                    .map_err(|error| error.code().to_string())?;
                run.snapshot()
                    .map_err(|error| error.code().to_string())?
                    .is_some_and(|snapshot| snapshot.status.is_terminal())
            };
            if terminal {
                return Ok(());
            }
            if pass.registered == 0
                && pass.claimed == 0
                && pass.delivered == 0
                && pass.cancelled == 0
            {
                return Ok(());
            }
        }
        Err("the dispatch pump did not quiesce".to_string())
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

/// A registry binding the test tool under the given spec.
fn tool_registry(spec: AgentEffectSpec) -> AgentToolRegistry {
    tool_registry_for_spec(TOOL, &spec)
}

/// A registry binding the test tool under the fail-safe unclassified default:
/// one non-idempotent attempt.
fn default_registry() -> AgentToolRegistry {
    tool_registry(AgentEffectSpec::non_idempotent())
}

// ---------------------------------------------------------------------------
// Scenario 5: dispatcher loss before durable `Started` safely redispatches.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatcher_loss_before_started_safely_redispatches() {
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new().with_turn(proposing_turn("resolved")),
        default_registry(),
        None,
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
    let model_spec = AgentEffectSpec::read_only()
        .with_max_attempts(2)
        .expect("the spec is valid");
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
        default_registry(),
        Some(model_spec),
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
        default_registry(),
        None,
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
    let registry = tool_registry(AgentEffectSpec::idempotent(3).expect("the spec is valid"));
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        registry,
        None,
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
    let registry =
        tool_registry(AgentEffectSpec::reconcileable(protocol, 3).expect("the spec is valid"));
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
        registry,
        None,
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
    let registry =
        tool_registry(AgentEffectSpec::reconcileable(protocol, 3).expect("the spec is valid"));
    let reconciler =
        ScriptedReconciler::new().with_finding(AgentReconciliationFinding::NotExecuted);
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        registry,
        None,
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
    let registry =
        tool_registry(AgentEffectSpec::reconcileable(protocol, 3).expect("the spec is valid"));
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
        registry,
        None,
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

/// The two ambiguous-recovery settlements that export nothing without this.
///
/// `park_indeterminate` was given a segment carrying
/// `rakka.agent.effect.status = indeterminate`, but the arms that reach the
/// *same* durable outcome by another route were not: the `NonIdempotent` arm
/// of `recover_ambiguous` hand-rolled the delivery, the counter, and the ticket
/// settlement without a segment, and `retry_ambiguous` closed none when the
/// ambiguous losses spent the last of the generation's budget. Between them
/// they cover the canonical case — a worker that dies after the durable
/// `Started` write — so a tail-sampling policy keyed on the status attribute
/// retained nothing for the outcome the attribute exists to select.
#[tokio::test]
async fn the_ambiguous_recovery_settlements_close_the_segments_that_select_them() {
    let segments = Arc::new(rakka_agent::InMemoryAgentSegmentSink::new());
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        default_registry(),
        None,
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    )
    .with_segments(segments.clone());
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

    assert_eq!(
        fx.effect_status(1).await,
        Some(AgentRunEffectStatus::Indeterminate),
        "the non-idempotent generation parks, or this proves nothing"
    );
    let parked: Vec<_> = segments
        .segments()
        .into_iter()
        .filter(|segment| {
            segment
                .attributes
                .get(rakka_agent::SEGMENT_ATTR_EFFECT_STATUS)
                == Some(&AgentRunEffectStatus::Indeterminate.as_label().to_string())
        })
        .collect();
    assert!(
        !parked.is_empty(),
        "the indeterminate park must close a segment a retention rule can select"
    );
    assert!(
        parked
            .iter()
            .all(|segment| segment.outcome == rakka_agent::AgentSegmentOutcome::Error),
        "an indeterminate outcome is an error event under 17.9"
    );
    assert!(
        parked
            .iter()
            .all(|segment| segment.error_code.as_deref() == Some("dispatcher-lost-after-started")),
        "and it names the stable code the loss produced: {parked:?}"
    );
}

/// The other half: an ambiguous loss that spends the generation's last attempt
/// settles `Exhausted`, and every attempt that could have described it was lost
/// before it closed a segment.
#[tokio::test]
async fn an_ambiguous_loss_that_exhausts_the_budget_closes_a_segment() {
    let segments = Arc::new(rakka_agent::InMemoryAgentSegmentSink::new());
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new().with_turn(proposing_turn("resolved")),
        default_registry(),
        None,
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    )
    .with_segments(segments.clone());
    fx.start().await;
    fx.settle().await;

    fx.probe.arm(AgentDispatchWindow::AfterStarted);
    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert!(pass.died, "the worker dies after the durable Started write");

    fx.expire_lease();
    fx.pump().await;

    assert_eq!(
        fx.effect_status(0).await,
        Some(AgentRunEffectStatus::Exhausted),
        "the single-attempt policy exhausts, or this proves nothing"
    );
    assert!(
        segments.segments().iter().any(|segment| {
            segment
                .attributes
                .get(rakka_agent::SEGMENT_ATTR_EFFECT_STATUS)
                == Some(&AgentRunEffectStatus::Exhausted.as_label().to_string())
                && segment.error_code.as_deref() == Some("dispatcher-lost-after-started")
        }),
        "the exhausting settlement must close a segment: {:?}",
        segments.operations()
    );
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
        default_registry(),
        None,
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
        default_registry(),
        None,
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

#[tokio::test]
async fn a_reconciled_indeterminate_generation_settles_its_attempts_exactly_once() {
    // The run's ledger settles a generation's attempt reservation at its
    // *first* resolution ([specification 9.7]) — for an ambiguous attempt, the
    // moment the `Indeterminate` outcome is recorded, because ambiguity does
    // not make an attempt free. A reconciliation that later confirms the
    // invocation executed changes what is *known*, not what was *attempted*,
    // so it must not bill the same attempts a second time: inflated
    // consumption would spuriously exhaust the run's `effect-attempts`
    // ceiling and travel upward in its settlement as spend nobody made.
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        default_registry(),
        None,
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

    // The generation parked, and its one `Started` attempt is already
    // consumed: one for the first model turn, one for the ambiguous tool call.
    let parked = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(parked.status, AgentRunStatus::WaitingForReconciliation);
    assert_eq!(parked.budget.consumption().effect_attempts, 2);

    let (effect_id, generation) = fx.parked_effect().await;
    let mut run = fx.fx.run();
    let now = fx.fx.now();
    run.recover(now).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::ResolveIndeterminateEffect {
            operation_id: rakka_agent::AgentOperationId::new(
                rakka_agent::AgentOperationKind::CheckpointResolution,
                [TENANT, AGENT, "resolve-52"],
            )
            .expect("the operation id derives"),
            effect_id,
            generation,
            resolution: Box::new(AgentEffectResolution::ConfirmedExecuted {
                outcome: Box::new(AgentRunEffectOutcome::Tool {
                    call_id: AgentToolCallId::new("call-1").expect("call id should be valid"),
                    content: AgentTaskContent::inline(serde_json::json!({ "receipt": "r-1" }))
                        .expect("the content is inline-bounded"),
                }),
            }),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("the resolution applies");

    fx.pump().await;

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    let consumed = run.budget.consumption();
    // Three effects reached dispatch — the first model turn, the reconciled
    // tool call, and the proposing model turn — and each made exactly one
    // attempt. The reconciliation itself billed nothing.
    assert_eq!(consumed.effects, 3);
    assert_eq!(
        consumed.effect_attempts, 3,
        "confirming an executed attempt must not bill it a second time"
    );
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "resolution recorded the outcome without re-invoking"
    );
}

#[tokio::test]
async fn a_redispatch_the_run_cannot_afford_is_refused() {
    // The redispatch half of the reservation discipline: a reconciliation that
    // proves an ambiguous generation never executed authorizes a *new*
    // generation with a fresh attempt budget, and that budget is reserved
    // before the generation becomes dispatchable — exactly as the original
    // turn's was ([specification 9.7]). A run whose attempt budget is already
    // spent cannot afford the re-invocation, so the resolution is refused, the
    // effect stays parked, and the operator's remaining decision is to cancel
    // the run, whose wind-down settles the generation without invocation.
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged")),
        default_registry(),
        None,
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.fx
        .instantiate_agent_with_envelope(envelope_for_registry(&fx.registry))
        .await;
    // Exactly the two attempts the first turn spends: one for its model call,
    // one for the ambiguous tool call. Nothing is left for a re-invocation.
    fx.fx
        .create_task_with(task_definition().with_budgets(AgentBudgetCeilings {
            max_effect_attempts: Some(2),
            ..AgentBudgetCeilings::unbounded()
        }))
        .await;

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
    let refusal = run
        .apply(
            AgentRunEntityCommand::ResolveIndeterminateEffect {
                operation_id: rakka_agent::AgentOperationId::new(
                    rakka_agent::AgentOperationKind::CheckpointResolution,
                    [TENANT, AGENT, "resolve-unaffordable"],
                )
                .expect("the operation id derives"),
                effect_id,
                generation,
                resolution: Box::new(AgentEffectResolution::ConfirmedNotExecuted),
            },
            &fx.fx.router,
            fx.fx.now(),
        )
        .await
        .expect_err("a re-invocation the run cannot afford is refused");
    assert_eq!(refusal.code(), "run-redispatch-unaffordable");

    // Nothing changed durably: the effect stays parked for a decision the run
    // can afford, and the target was never re-invoked.
    assert_eq!(
        fx.effect_status(1).await,
        Some(AgentRunEffectStatus::Indeterminate)
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
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
        default_registry(),
        None,
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
        default_registry(),
        None,
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
    let registry = tool_registry(
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
        registry,
        None,
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

    let fleet = {
        use rakka_persistence::DurableStateStore;
        fx.fleet_store
            .load(&rakka_agent_workflow::agent_dispatcher_fleet_persistence_id())
            .await
            .expect("the fleet state loads")
            .expect("the fleet holds the claim's entry")
    };
    let encoded = serde_json::to_string(&fleet.state).expect("the fleet serializes");
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
    let model_spec = AgentEffectSpec::read_only()
        .with_max_attempts(3)
        .expect("the spec is valid");
    let fx = DispatchFixture::new(
        adapter,
        default_registry(),
        Some(model_spec),
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

/// The M1 per-turn durable-write budget: what one clean model turn — accept
/// the assignment, commit the model effect, ticket it, claim it, invoke,
/// deliver the turn, propose the result, accept it, settle the escrow —
/// costs each durable store, measured from task creation through the settled
/// escrow.
///
/// These are exact counts, not ceilings, and the assertions below are
/// deliberate change-detectors: phases 3-5 multiply write load, so a
/// legitimate pipeline change must re-derive the number consciously — update
/// the constant *and* the recorded budget in
/// `examples/durable-agent-acceptance/README.md` together — rather than let
/// the per-turn cost creep silently. Wall-clock latency is printed but never
/// asserted; the release-build numbers are recorded in the same README, with
/// this test as the reproduction command.
const TURN_BUDGET_RUN_STORE_WRITES: usize = 10;
/// The task store's share of the budget: creation with its assignment
/// decision, the run's acceptance, the result proposal's decision, and the
/// escrow settlement and return.
const TURN_BUDGET_TASK_STORE_WRITES: usize = 8;
/// The workflow outbox's share: register the ticket, mark it dispatching,
/// settle it.
const TURN_BUDGET_WORKFLOW_STORE_WRITES: usize = 3;

#[tokio::test]
async fn per_turn_durable_write_count_and_latency_measurement() {
    // One clean model turn, end to end over the integrated pipeline. The
    // fixture instantiates the agent first; the counters reset there, so the
    // budget is exactly "one accepted turn, creation through settled escrow".
    let fx = DispatchFixture::new(
        DeterministicModelAdapter::new().with_turn(proposing_turn("resolved")),
        default_registry(),
        None,
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    );
    fx.fx
        .instantiate_agent_with_envelope(envelope_for_registry(&fx.registry))
        .await;
    fx.fx.runs.reset_writes();
    fx.fx.tasks.reset_writes();
    fx.workflow_store.reset_writes();

    let started = std::time::Instant::now();
    fx.fx.create_task().await;
    fx.pump().await;
    let elapsed = started.elapsed();

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);

    // Every store counts its compare-and-sets directly. The fleet store is
    // deliberately not budgeted: its lease bookkeeping scales with worker
    // churn, not with turns.
    let run_writes = fx.fx.runs.writes();
    let task_writes = fx.fx.tasks.writes();
    let workflow_writes = fx.workflow_store.writes();
    println!(
        "one full turn (accept -> model effect -> ticket -> claim -> invoke -> deliver -> \
         propose -> accept): run-store writes = {run_writes}, task-store writes = \
         {task_writes}, workflow-store writes = {workflow_writes}, wall time = {elapsed:?}"
    );

    assert_eq!(
        run_writes, TURN_BUDGET_RUN_STORE_WRITES,
        "the per-turn run-store budget moved; a deliberate pipeline change must re-derive \
         the budget constant and the example README together"
    );
    assert_eq!(
        task_writes, TURN_BUDGET_TASK_STORE_WRITES,
        "the per-turn task-store budget moved; a deliberate pipeline change must re-derive \
         the budget constant and the example README together"
    );
    assert_eq!(
        workflow_writes, TURN_BUDGET_WORKFLOW_STORE_WRITES,
        "the per-turn workflow-store budget moved; a deliberate pipeline change must \
         re-derive the budget constant and the example README together"
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

// ---------------------------------------------------------------------------
// The owner-kill sweeps at the pipeline's own stores: the workflow outbox and
// the dispatcher fleet. Scenarios 5-10 above kill the *dispatcher* at its
// windows; these kill the *store owner* at every durable write instead, which
// is what a pod loss looks like to the outbox and fleet records themselves.
// ---------------------------------------------------------------------------

/// A dispatch fixture whose model and tool are both idempotent, so every
/// ambiguous window retries under the generation's single external key and
/// every sweep iteration converges on completion.
fn idempotent_pipeline_fixture() -> DispatchFixture {
    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn(TOOL))
        .with_turn_for(2, proposing_turn("charged"))
        .with_retry_policy(rakka_agent::AgentModelRetryPolicy {
            safety_class: rakka_agent::AgentEffectSafetyClass::Idempotent,
            max_attempts: 3,
        })
        .expect("the adapter policy is valid");
    DispatchFixture::new(
        adapter,
        tool_registry(AgentEffectSpec::idempotent(3).expect("the spec is valid")),
        Some(AgentEffectSpec::idempotent(3).expect("the model spec is valid")),
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    )
}

/// The distinct external idempotency keys the tool saw.
fn distinct_tool_keys(fx: &DispatchFixture) -> std::collections::BTreeSet<String> {
    fx.tools
        .invocations()
        .into_iter()
        .filter(|invocation| invocation.tool == TOOL)
        .map(|invocation| invocation.idempotency_key)
        .collect()
}

#[tokio::test]
async fn the_pipeline_survives_any_outbox_store_loss_under_one_idempotency_key() {
    // Kill the workflow-outbox store's owner at every durable write of the
    // ticket lifecycle — register, dispatch, settle — on both sides of the
    // compare-and-set. Every crash converges on one completed run, and the
    // external system only ever saw the one key the first attempt minted:
    // exactly-once ticket registration is what makes that true.
    let reference = idempotent_pipeline_fixture();
    reference.start().await;
    reference
        .try_pump()
        .await
        .expect("the reference flow completes");
    let writes = reference.workflow_store.writes();
    assert!(
        writes >= 4,
        "the ticket lifecycle should make several durable writes, saw {writes}"
    );

    rakka_agent::testkit::sweep_crash_points(writes, |nth, point| async move {
        let fx = idempotent_pipeline_fixture();
        fx.fx
            .instantiate_agent_with_envelope(envelope_for_registry(&fx.registry))
            .await;

        fx.workflow_store.crash_at(nth, point);
        fx.fx.create_task().await;
        let _crashed = fx.try_pump().await;

        // A new owner activates; the dead pass's lease lapses.
        fx.workflow_store.assert_crash_fired(nth, point);
        fx.workflow_store.survive();
        fx.expire_lease();
        fx.try_pump().await.unwrap_or_else(|error| {
            panic!("crash {point:?} at write {nth} did not converge: {error}")
        });

        let run = fx.fx.run_snapshot().await.expect("the run exists");
        assert_eq!(
            run.status,
            AgentRunStatus::Completed,
            "crash {point:?} at write {nth} should still complete"
        );
        assert_eq!(
            run.turn, 2,
            "crash {point:?} at write {nth} replayed a turn"
        );
        assert_eq!(
            distinct_tool_keys(&fx).len(),
            1,
            "crash {point:?} at write {nth} invoked under a second idempotency key"
        );
    })
    .await;
}

#[tokio::test]
async fn the_pipeline_survives_any_fleet_store_loss_under_one_idempotency_key() {
    // The same sweep over the dispatcher-fleet store: worker registration,
    // claims, and failure records. A fleet record is a fence, never proof of
    // an external outcome — so losing its owner at any write may delay the
    // attempt but must not duplicate the external effect.
    let reference = idempotent_pipeline_fixture();
    reference.start().await;
    reference
        .try_pump()
        .await
        .expect("the reference flow completes");
    let writes = reference.fleet_store.writes();
    assert!(
        writes >= 2,
        "the fleet lifecycle should make durable writes, saw {writes}"
    );

    rakka_agent::testkit::sweep_crash_points(writes, |nth, point| async move {
        let fx = idempotent_pipeline_fixture();
        fx.fx
            .instantiate_agent_with_envelope(envelope_for_registry(&fx.registry))
            .await;

        fx.fleet_store.crash_at(nth, point);
        fx.fx.create_task().await;
        let _crashed = fx.try_pump().await;

        fx.fleet_store.assert_crash_fired(nth, point);
        fx.fleet_store.survive();
        fx.expire_lease();
        fx.try_pump().await.unwrap_or_else(|error| {
            panic!("crash {point:?} at write {nth} did not converge: {error}")
        });

        let run = fx.fx.run_snapshot().await.expect("the run exists");
        assert_eq!(
            run.status,
            AgentRunStatus::Completed,
            "crash {point:?} at write {nth} should still complete"
        );
        assert_eq!(
            run.turn, 2,
            "crash {point:?} at write {nth} replayed a turn"
        );
        assert_eq!(
            distinct_tool_keys(&fx).len(),
            1,
            "crash {point:?} at write {nth} invoked under a second idempotency key"
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// Scenario 23's dispatcher-restart half: the persisted trace segments and the
// durable outcome are identical across a dispatcher kill at every window —
// including the delivered-but-unsettled one, where the fresh worker must
// settle the row its re-read intent shows resolved, not invoke the target
// again.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatcher_restart_preserves_segments_and_outcome_at_every_window() {
    const INGRESS_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    fn traced_fixture() -> DispatchFixture {
        let adapter = DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn(TOOL))
            .with_turn_for(2, proposing_turn("charged"))
            .with_retry_policy(
                rakka_agent::AgentModelRetryPolicy::read_only(3).expect("the policy is valid"),
            )
            .expect("the adapter accepts the policy");
        DispatchFixture::new(
            adapter,
            tool_registry(
                AgentEffectSpec::read_only()
                    .with_max_attempts(3)
                    .expect("the spec is valid"),
            ),
            Some(
                AgentEffectSpec::read_only()
                    .with_max_attempts(3)
                    .expect("the model spec is valid"),
            ),
            RecordingToolExecutor::new(),
            ScriptedReconciler::new(),
        )
    }

    async fn drive_traced(fx: &DispatchFixture) {
        fx.fx
            .instantiate_agent_with_envelope(envelope_for_registry(&fx.registry))
            .await;
        fx.fx
            .create_task_traced(rakka_agent_workflow::AgentTelemetryContext {
                trace_parent: Some(INGRESS_PARENT.to_string()),
                ..rakka_agent_workflow::AgentTelemetryContext::default()
            })
            .await;
    }

    async fn segments_and_outcome(fx: &DispatchFixture) -> (String, AgentRunStatus, u64) {
        let view = rakka_agent::assemble_agent_session_view(
            &fx.fx.runs,
            &run_scope(),
            &rakka_agent::AgentSchemaPolicy::default(),
            None,
            rakka_agent_workflow::AgentTimestampMillis::new(9_999),
        )
        .await
        .expect("the view assembles")
        .expect("the run exists");
        let segments = serde_json::to_string(&view.trace_segments).expect("the segments serialize");
        let run = fx.fx.run_snapshot().await.expect("the run exists");
        (segments, run.status, run.turn)
    }

    // The unkilled reference fixes the segments and outcome every restart
    // must reproduce.
    let reference = traced_fixture();
    drive_traced(&reference).await;
    reference.pump().await;
    let expected = segments_and_outcome(&reference).await;
    assert_eq!(expected.1, AgentRunStatus::Completed);

    for window in [
        AgentDispatchWindow::BeforeStarted,
        AgentDispatchWindow::AfterStarted,
        AgentDispatchWindow::AfterInvocation,
        AgentDispatchWindow::AfterResultDelivery,
    ] {
        let fx = traced_fixture();
        drive_traced(&fx).await;
        fx.pump_until_tool_ticket().await;

        fx.probe.arm(window);
        let pass = fx
            .pipeline()
            .pump_run(&run_scope())
            .await
            .expect("the pass runs");
        assert!(pass.died, "the probe kills the worker at {window:?}");
        let calls_at_death = fx.tools.invocation_count(TOOL);

        // A fresh worker recovers from durable state alone.
        fx.expire_lease();
        fx.pump().await;

        let observed = segments_and_outcome(&fx).await;
        assert_eq!(
            observed, expected,
            "a dispatcher restart at {window:?} changed a segment or the outcome"
        );

        if window == AgentDispatchWindow::AfterResultDelivery {
            // The run already durably held the tool's word when the worker
            // died, so segment/outcome equality alone cannot tell settlement
            // apart from a re-invocation the run's dedup absorbed. Recovery
            // must settle the delivered-but-unsettled row from the re-read
            // intent without touching the target again, and must not leave
            // the ticket claimable forever.
            assert_eq!(
                fx.tools.invocation_count(TOOL),
                calls_at_death,
                "recovery re-invoked a tool whose result the run already holds"
            );
            fx.expire_lease();
            let sweep = fx
                .pipeline()
                .pump_run(&run_scope())
                .await
                .expect("the post-recovery pass runs");
            assert_eq!(
                sweep.claimed, 0,
                "recovery left the delivered ticket claimable"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Slice 2.1: a memory promotion travels the production dispatch path — the
// authority's promotion arm, the real outbox ticket, the executor invocation,
// and convergence when a dispatch pass dies mid-flight. The worker is killed
// at the pass's first `AfterInvocation` window (the model call: its ticket
// precedes the promotion's, because the selection's first sequence commits
// with the first model effect); the recovered worker re-drives both tickets,
// and the promotion's purely derived identities leave exactly one memory
// however the pass was interrupted. The run may complete before the
// promotion's result lands — the documented terminal-refusal convergence —
// and the store, not the receipt, is the source of truth it converges on.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_promotion_survives_dispatcher_loss_mid_pass() {
    use rakka_agent::{
        promotion_operation_id, AgentMemoryPromotionRequest, AgentPrivateMemoryKind,
        AgentPrivateMemoryStore, AgentRevisionNumber, AgentRunMemory,
        InMemoryAgentPrivateMemoryStore, InMemoryContextSnapshotStore, InMemorySessionMemoryStore,
        MemorySequence, PrivateMemoryCursor, SessionMemoryPromotionExecutor,
    };
    use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};

    let session = Arc::new(InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(InMemoryContextSnapshotStore::new());
    let private = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    // Two read-only attempts, declared by the adapter and mirrored by the
    // spec: the armed death interrupts the model invocation, and the
    // recovered worker retries it under policy instead of exhausting the
    // turn.
    let adapter = DeterministicModelAdapter::new()
        .with_turn(proposing_turn("resolved"))
        .with_retry_policy(
            rakka_agent::AgentModelRetryPolicy::read_only(2).expect("the policy is valid"),
        )
        .expect("the adapter accepts the policy");
    let fx = DispatchFixture::new(
        adapter,
        default_registry(),
        Some(
            AgentEffectSpec::read_only()
                .with_max_attempts(2)
                .expect("the model spec is valid"),
        ),
        RecordingToolExecutor::new(),
        ScriptedReconciler::new(),
    )
    .with_promotions(
        AgentRunMemory::new(session.clone(), snapshots).with_private_store(private.clone()),
        Arc::new(SessionMemoryPromotionExecutor::new(
            session.clone(),
            private.clone(),
        )),
    );
    fx.start().await;
    fx.settle().await;

    // Commit the promotion of the task's input (sequence one) while the run
    // waits on its model call.
    let scope = run_scope();
    let mut run = fx.fx.run();
    let now = fx.fx.now();
    run.recover(now).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::PromoteMemory {
            operation_id: promotion_operation_id(&scope, "policy-1").expect("operation id"),
            promotion: Box::new(AgentMemoryPromotionRequest {
                from_sequence: MemorySequence::new(1),
                to_sequence: MemorySequence::new(1),
                kind: AgentPrivateMemoryKind::Semantic,
                target: None,
                confidence_bps: 9_000,
                requested_by: PrincipalRef {
                    principal_type: "service".to_string(),
                    principal_id: "memory-curator".to_string(),
                    display_name: None,
                },
            }),
        },
        &fx.fx.router,
        now,
    )
    .await
    .expect("the promotion applies");

    // The worker dies mid-pass, after an invocation committed and before its
    // receipt was recorded anywhere.
    fx.probe.arm(AgentDispatchWindow::AfterInvocation);
    let _ = fx.try_pump().await;
    assert_eq!(fx.probe.deaths(), 1, "the armed window fired");

    // A fresh worker recovers from durable state alone and re-drives every
    // ticket the dead pass left: the model call retries under its policy, the
    // promotion executes under its derived operation ids, and the pipeline
    // settles even when the run completes before the promotion's result lands.
    fx.expire_lease();
    fx.pump().await;

    let owner = agent_scope();
    assert_eq!(
        private.len(&owner),
        1,
        "the redelivered promotion converged on one memory"
    );
    let listed = private
        .list(
            &owner,
            PrivateMemoryCursor::start(),
            AgentTimestampMillis::new(1_000_000),
        )
        .await
        .expect("list");
    assert_eq!(listed.memories.len(), 1);
    assert_eq!(
        listed.memories[0].revision,
        AgentRevisionNumber::INITIAL,
        "the second attempt replayed rather than re-wrote"
    );

    let snapshot = fx.fx.run_snapshot().await.expect("the run exists");
    assert!(
        snapshot.status.is_terminal(),
        "the run settled despite the dispatcher loss: {:?}",
        snapshot.status
    );
}
