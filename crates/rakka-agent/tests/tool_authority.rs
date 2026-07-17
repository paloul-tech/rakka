//! The tool authority layers over the real dispatch pipeline.
//!
//! Specification: sections 11.7, 11.8, and 16; scenarios 44 and 54 of
//! section 18. Every test drives the production path: the run commits the
//! effect intent, the ticket reaches the durable outbox, the dispatcher
//! fleet leases it — and the [`rakka_agent::AgentDispatchAuthority`] decides,
//! before durable `Started`, whether the attempt may invoke anything.
//!
//! Scenario 54 is the shape of most tests here: a *model-visible* tool call —
//! declared, registered, sitting in the model's descriptor list — stays
//! undispatchable when its binding, grant, credential, checkpoint,
//! execution-policy, or immediate-safety check fails. The
//! [`RecordingToolExecutor`]'s invocation log is the external system: zero
//! recorded invocations is what "undispatchable" means, and the durable
//! failure code names the check that refused.

use std::collections::BTreeSet;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use rakka_agent::testkit::{
    DeterministicModelAdapter, InProcessRunResultDelivery, RecordingToolExecutor,
    ScriptedDispatcher, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentAuthorityEnvelope, AgentDefinition, AgentDefinitionId, AgentDefinitionRevision,
    AgentDispatchPass, AgentEffectSpec, AgentEntityAuthority, AgentEntityCommand, AgentEntityStore,
    AgentExecutionPolicyRef, AgentExecutionPolicyRouter, AgentGuardrail, AgentGuardrailBoundary,
    AgentGuardrailChain, AgentGuardrailOutcome, AgentGuardrailStage, AgentGuardrailStageId,
    AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRevisionNumber,
    AgentRunEffectDispatcher, AgentRunStatus, AgentRunTerminalReason, AgentSettings,
    AgentSettingsChange, AgentSetupRevision, AgentTaskContent, AgentToolAuthority, AgentToolCallId,
    AgentToolCallRequest, AgentToolDeclaration, AgentToolId, AgentToolRegistry,
    WorkflowAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::WorkflowState;
use rakka_agent_workflow::{
    AgentDispatcherFleetSettings, AgentDispatcherFleetState, AgentDispatcherWorkerId,
};
use rakka_persistence::InMemoryDurableStateStore;
use serde_json::Value;

mod common;

use common::*;

type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;
type FleetStore = InMemoryDurableStateStore<AgentDispatcherFleetState>;
type WorkflowSink = WorkflowAgentRunEffectSink<WorkflowStore, SharedAtomicWorkflowClock>;
type Pipeline =
    AgentRunEffectDispatcher<WorkflowStore, FleetStore, RunStore, SharedAtomicWorkflowClock>;

const LEASE_MS: u64 = 60_000;
const TOOL: &str = "charge-card";

fn tool_id() -> AgentToolId {
    AgentToolId::new(TOOL).expect("tool id should be valid")
}

fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me do that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                tool_id(),
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

fn stage_id(id: &str) -> AgentGuardrailStageId {
    AgentGuardrailStageId::new(id).expect("the stage id is valid")
}

/// The authority fixture: the common task-and-run fixture over the durable
/// workflow-outbox sink, plus the fleet, the executor, and a configurable
/// [`AgentToolAuthority`] behind the pipeline's required gate.
struct AuthorityFixture {
    fx: Fixture<DeterministicModelAdapter, WorkflowSink>,
    adapter: DeterministicModelAdapter,
    registry: AgentToolRegistry,
    authority: AgentToolAuthority,
    setup: Option<AgentSetupRevision>,
    envelope: AgentAuthorityEnvelope,
    workflow_store: WorkflowStore,
    fleet_store: FleetStore,
    wf_clock: SharedAtomicWorkflowClock,
    tools: RecordingToolExecutor,
}

impl AuthorityFixture {
    fn new(
        adapter: DeterministicModelAdapter,
        registry: AgentToolRegistry,
        model_spec: Option<AgentEffectSpec>,
    ) -> Self {
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
        let envelope = envelope_for_registry(&registry);
        let authority = AgentToolAuthority::new(registry.clone());
        Self {
            fx,
            adapter,
            registry,
            authority,
            setup: None,
            envelope,
            workflow_store,
            fleet_store,
            wf_clock,
            tools: RecordingToolExecutor::new(),
        }
    }

    /// Replaces the authority the pipeline consults.
    fn with_authority(mut self, authority: AgentToolAuthority) -> Self {
        self.authority = authority;
        self
    }

    /// Replaces the envelope the agent is instantiated under.
    fn with_envelope(mut self, envelope: AgentAuthorityEnvelope) -> Self {
        self.envelope = envelope;
        self
    }

    /// Enforces the given run setup at dispatch.
    fn with_setup(mut self, setup: AgentSetupRevision) -> Self {
        self.setup = Some(setup);
        self
    }

    async fn start(&self) {
        self.fx
            .instantiate_agent_with_envelope(self.envelope.clone())
            .await;
        self.fx.create_task().await;
    }

    /// A fresh dispatch worker over the shared durable stores.
    fn pipeline(&self) -> Pipeline {
        let mut gate = AgentEntityAuthority::new(self.fx.agents.clone(), self.authority.clone());
        if let Some(setup) = &self.setup {
            gate = gate.with_setup(setup.clone());
        }
        AgentRunEffectDispatcher::new(
            AgentDispatcherWorkerId::new("worker-1"),
            self.workflow_store.clone(),
            self.fleet_store.clone(),
            self.fx.runs.clone(),
            self.wf_clock.clone(),
            Arc::new(self.adapter.clone()),
            Arc::new(self.tools.clone()),
            Arc::new(gate),
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

    /// One settle-and-dispatch round.
    async fn one_pass(&self) -> AgentDispatchPass {
        self.settle().await;
        self.pipeline()
            .pump_run(&run_scope())
            .await
            .expect("the dispatch pass runs")
    }

    /// Drives entities and the dispatch pipeline until the run is terminal or
    /// nothing moves. Each round uses a fresh worker.
    async fn pump(&self) {
        for _round in 0..16 {
            let pass = self.one_pass().await;
            let snapshot = self.fx.run_snapshot().await;
            let terminal = snapshot
                .as_ref()
                .is_some_and(|run| run.status.is_terminal());
            if terminal {
                self.settle().await;
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

    /// The stable code of the effect failure that stopped the run.
    async fn terminal_failure_code(&self) -> String {
        let run = self.fx.run_snapshot().await.expect("the run exists");
        assert_eq!(
            run.status,
            AgentRunStatus::Failed,
            "the run should have stopped on the refused effect"
        );
        match run.terminal_reason {
            Some(AgentRunTerminalReason::EffectFailed { code, .. }) => code,
            other => panic!("expected an effect failure, found {other:?}"),
        }
    }

    /// Applies a settings update to the agent entity — an immediate-safety
    /// change once it carries a revocation.
    async fn apply_settings(&self, discriminator: &str, changes: Vec<AgentSettingsChange>) {
        let mut agent = AgentEntityStore::new(agent_scope(), self.fx.agents.clone());
        agent.recover().await.expect("the agent recovers");
        let expected_revision = agent
            .state()
            .expect("the state reads")
            .expect("the agent exists")
            .settings()
            .revision();
        agent
            .apply(AgentEntityCommand::UpdateSettings {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::SettingsUpdate,
                    &agent_scope(),
                    discriminator,
                )
                .expect("operation id should be derivable"),
                expected_revision,
                changes,
                provenance: Box::new(provenance(90)),
            })
            .await
            .expect("the settings update applies");
    }

    /// Suspends or resumes the agent through its lifecycle protocol.
    async fn set_suspended(&self, suspended: bool, discriminator: &str) {
        let mut agent = AgentEntityStore::new(agent_scope(), self.fx.agents.clone());
        agent.recover().await.expect("the agent recovers");
        let expected_lifecycle_revision = agent
            .state()
            .expect("the state reads")
            .expect("the agent exists")
            .lifecycle_revision();
        let operation_id = AgentOperationId::for_agent(
            AgentOperationKind::LifecycleCommand,
            &agent_scope(),
            discriminator,
        )
        .expect("operation id should be derivable");
        let command = if suspended {
            AgentEntityCommand::Suspend {
                operation_id,
                expected_lifecycle_revision,
                provenance: Box::new(provenance(91)),
            }
        } else {
            AgentEntityCommand::Resume {
                operation_id,
                expected_lifecycle_revision,
                provenance: Box::new(provenance(92)),
            }
        };
        agent
            .apply(command)
            .await
            .expect("the lifecycle command applies");
    }
}

/// An adapter scripted for one tool turn and one closing proposal.
fn tool_then_proposal() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn())
        .with_turn_for(2, proposing_turn("charged"))
}

// ---------------------------------------------------------------------------
// Scenario 54: the binding check. A tool the deployment never registered is
// undispatchable, however plausibly the model asked for it.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_unregistered_tool_call_is_undispatchable() {
    // The definition declares the tool, so the intent commits — but no binding
    // exists, so no authority layer can vouch for what the call would execute.
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope.task_definitions.insert(task_definition_id());
    envelope.tools.insert(
        tool_id(),
        AgentToolDeclaration::new(rakka_agent::AgentEffectSafetyClass::NonIdempotent),
    );
    let fx = AuthorityFixture::new(tool_then_proposal(), AgentToolRegistry::new(), None)
        .with_envelope(envelope);
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "tool-binding-missing");
    assert_eq!(fx.tools.invocation_count(TOOL), 0, "nothing was invoked");
}

// ---------------------------------------------------------------------------
// Scenario 54 / 44: the envelope check. A registered tool the definition
// never declared is undispatchable — registration is deployment truth, not
// agent authority.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_registered_but_undeclared_tool_is_undispatchable() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope.task_definitions.insert(task_definition_id());
    let fx = AuthorityFixture::new(tool_then_proposal(), registry, None).with_envelope(envelope);
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "undeclared-tool");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// Scenario 54: the immediate-safety check. A model-visible tool revoked by a
// settings update stays undispatchable from the very next attempt.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_model_visible_tool_is_undispatchable_once_revoked() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let fx = AuthorityFixture::new(tool_then_proposal(), registry, None);

    // The tool is model-visible before the revocation: declared, registered,
    // and unrevoked. Visibility is exactly what scenario 54 says is not
    // authority.
    assert_eq!(
        fx.registry
            .model_visible(&fx.envelope, &AgentSettings::default())
            .len(),
        1
    );

    fx.start().await;
    fx.apply_settings(
        "revoke-tool",
        vec![AgentSettingsChange::RevokeTool(tool_id())],
    )
    .await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "tool-revoked");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// Scenario 54: the credential check. A revoked credential binding keeps the
// tool undispatchable before any resolution could happen.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_revoked_credential_binding_is_undispatchable() {
    let credential =
        rakka_agent::AgentCredentialBindingRef::new("payments").expect("the binding ref is valid");
    let spec = AgentEffectSpec::idempotent(2)
        .expect("the spec is valid")
        .with_credential_binding(credential.clone());
    let registry = tool_registry_for_spec(TOOL, &spec);
    let fx = AuthorityFixture::new(tool_then_proposal(), registry, None);
    fx.start().await;
    fx.apply_settings(
        "revoke-credential",
        vec![AgentSettingsChange::RevokeCredentialBinding(credential)],
    )
    .await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "credential-revoked");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// Scenario 54: the checkpoint check. A binding that requires an effect-bound
// checkpoint grant fails closed until the slice 1.10 checkpoint runtime can
// issue one.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_checkpoint_requiring_tool_is_undispatchable_without_a_grant() {
    let registry = AgentToolRegistry::new()
        .register(
            tool_binding_for_spec(TOOL, &AgentEffectSpec::non_idempotent())
                .with_checkpoint_required(),
        )
        .expect("the tool registers");
    let fx = AuthorityFixture::new(tool_then_proposal(), registry, None);
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "checkpoint-required");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// Scenario 54: the execution-policy check. An intent routed through a trust
// class no configured executor accepts stays undispatchable; a router that
// accepts the class lets the same intent execute.
// ---------------------------------------------------------------------------

struct AcceptClass(&'static str);

impl AgentExecutionPolicyRouter for AcceptClass {
    fn accepts(&self, policy: &AgentExecutionPolicyRef) -> bool {
        policy.as_str() == self.0
    }
}

#[tokio::test]
async fn an_execution_policy_no_executor_accepts_is_undispatchable() {
    let policy = AgentExecutionPolicyRef::new("sandboxed").expect("the policy ref is valid");
    let spec = AgentEffectSpec::non_idempotent().with_execution_policy(policy);
    let registry = tool_registry_for_spec(TOOL, &spec);

    // No router configured: fail closed rather than run with ambient
    // authority.
    let fx = AuthorityFixture::new(tool_then_proposal(), registry.clone(), None);
    fx.start().await;
    fx.pump().await;
    assert_eq!(
        fx.terminal_failure_code().await,
        "execution-policy-unroutable"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);

    // A router that accepts the class routes the same intent to execution.
    let routed = AuthorityFixture::new(tool_then_proposal(), registry.clone(), None)
        .with_authority(
            AgentToolAuthority::new(registry)
                .with_execution_router(Arc::new(AcceptClass("sandboxed"))),
        );
    routed.start().await;
    routed.pump().await;
    let run = routed.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(routed.tools.invocation_count(TOOL), 1);
}

// ---------------------------------------------------------------------------
// Scenario 54: grant validity. A grant whose window has already closed is
// rechecked — and refused — before the attempt.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_expired_grant_is_undispatchable() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let fx = AuthorityFixture::new(tool_then_proposal(), registry.clone(), None)
        .with_authority(AgentToolAuthority::new(registry).with_grant_ttl_ms(0));
    fx.start().await;
    fx.pump().await;

    // The model call is the first dispatch the zero-width grant window
    // refuses; nothing external was ever invoked.
    assert_eq!(fx.terminal_failure_code().await, "grant-expired");
    assert_eq!(fx.adapter.calls(), 0);
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// Scenario 54 / 53: the immediate-safety suspension. A suspended agent
// dispatches nothing — the refusal burns an attempt rather than failing the
// effect, so resuming the agent lets the very next attempt proceed.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_suspended_agent_dispatches_nothing_until_resumed() {
    let adapter = DeterministicModelAdapter::new()
        .with_turn(proposing_turn("resolved"))
        .with_retry_policy(
            rakka_agent::AgentModelRetryPolicy::read_only(3).expect("the policy is valid"),
        )
        .expect("the adapter accepts the policy");
    let model_spec = AgentEffectSpec::read_only()
        .with_max_attempts(3)
        .expect("the spec is valid");
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let fx = AuthorityFixture::new(adapter, registry, Some(model_spec));
    fx.start().await;
    fx.set_suspended(true, "suspend-1").await;

    // One pass under suspension: the ticket is claimed, the authority refuses
    // before durable `Started`, and nothing is invoked.
    let pass = fx.one_pass().await;
    assert!(pass.claimed >= 1, "the ticket was claimed");
    assert_eq!(pass.invoked, 0, "nothing was invoked under suspension");
    assert_eq!(fx.adapter.calls(), 0);
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert!(
        !run.status.is_terminal(),
        "a transient refusal does not fail the run"
    );

    // Resuming clears the condition; the next attempt rechecks and proceeds.
    fx.set_suspended(false, "resume-1").await;
    fx.pump().await;
    assert_eq!(fx.adapter.calls(), 1, "exactly one model invocation");
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

// ---------------------------------------------------------------------------
// Scenario 44: the mandatory-guardrail rule at dispatch. An envelope that
// requires a stage the deployment's chain cannot run fails closed rather
// than dispatching without the guardrail the definition promised.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_missing_mandatory_guardrail_stage_fails_closed_at_dispatch() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let mut envelope = envelope_for_registry(&registry);
    envelope.mandatory_guardrails.insert(stage_id("pii-filter"));
    let fx = AuthorityFixture::new(tool_then_proposal(), registry, None).with_envelope(envelope);
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "guardrail-stage-missing");
    assert_eq!(fx.adapter.calls(), 0, "the model boundary is guarded too");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// Scenario 44: a run setup that narrowed the definition is enforced at
// dispatch, not only at construction.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_setup_narrowing_away_a_tool_is_enforced_at_dispatch() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let envelope = envelope_for_registry(&registry);
    let definition = AgentDefinitionRevision::initial(
        AgentDefinition::new(
            AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
            "Resolves customer support tickets end to end.",
            envelope.clone(),
        )
        .expect("the definition is valid"),
        provenance(1),
    );
    // The setup is a *legal* narrowing — it simply selects no tools — so
    // construction accepts it, and dispatch is where it must bite.
    let mut narrowed = AgentAuthorityEnvelope::empty();
    narrowed.task_definitions.insert(task_definition_id());
    let setup = AgentSetupRevision::new(
        AgentRevisionNumber::INITIAL,
        &definition,
        narrowed,
        provenance(2),
    )
    .expect("the narrowing is legal");

    let fx = AuthorityFixture::new(tool_then_proposal(), registry, None)
        .with_envelope(envelope)
        .with_setup(setup);
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "setup-excludes-tool");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// Scenario 54 / specification 16: a guardrail block at the tool boundary
// keeps the call undispatchable.
// ---------------------------------------------------------------------------

struct BlockLargeAmounts;

impl AgentGuardrail for BlockLargeAmounts {
    fn evaluate(&self, _: AgentGuardrailBoundary, content: &Value) -> AgentGuardrailOutcome {
        let amount = content.get("amount").and_then(Value::as_u64).unwrap_or(0);
        if amount > 10 {
            AgentGuardrailOutcome::Block {
                reason_code: "amount-over-limit".to_string(),
                evidence: None,
            }
        } else {
            AgentGuardrailOutcome::Allow
        }
    }
}

#[tokio::test]
async fn a_guardrail_block_keeps_a_tool_call_undispatchable() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
        .with_stage(
            AgentGuardrailStage::new(
                stage_id("amount-limit"),
                AgentRevisionNumber::INITIAL,
                Arc::new(BlockLargeAmounts),
            )
            .at_boundary(AgentGuardrailBoundary::ToolRequest)
            .mandatory(),
        )
        .expect("the stage registers");
    let fx = AuthorityFixture::new(tool_then_proposal(), registry.clone(), None)
        .with_authority(AgentToolAuthority::new(registry).with_guardrails(chain.clone()));
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "guardrail-blocked");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);

    // The deployment-mandatory stage cannot be narrowed away by any
    // definition or setup: the removal operation itself refuses.
    let error = chain
        .narrowed(&BTreeSet::from([stage_id("amount-limit")]))
        .expect_err("a mandatory stage cannot be removed");
    assert_eq!(error.code(), "guardrail-mandatory-stage-immutable");
}

// ---------------------------------------------------------------------------
// Specification 16: a deterministic guardrail transform replaces the
// arguments the executor sees, and the durable intent is untouched — every
// retry re-derives the identical transformed input.
// ---------------------------------------------------------------------------

struct ClampAmount;

impl AgentGuardrail for ClampAmount {
    fn evaluate(&self, _: AgentGuardrailBoundary, content: &Value) -> AgentGuardrailOutcome {
        let amount = content.get("amount").and_then(Value::as_u64).unwrap_or(0);
        if amount > 10 {
            AgentGuardrailOutcome::Transform {
                content: serde_json::json!({ "amount": 10 }),
                reason_code: "amount-clamped".to_string(),
            }
        } else {
            AgentGuardrailOutcome::Allow
        }
    }
}

#[tokio::test]
async fn a_guardrail_transform_reaches_the_executor_deterministically() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
        .with_stage(
            AgentGuardrailStage::new(
                stage_id("amount-clamp"),
                AgentRevisionNumber::INITIAL,
                Arc::new(ClampAmount),
            )
            .at_boundary(AgentGuardrailBoundary::ToolRequest),
        )
        .expect("the stage registers");
    let fx = AuthorityFixture::new(tool_then_proposal(), registry.clone(), None)
        .with_authority(AgentToolAuthority::new(registry).with_guardrails(chain));
    fx.start().await;
    fx.pump().await;

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    let invocations = fx.tools.invocations();
    assert_eq!(invocations.len(), 1);
    assert_eq!(
        invocations[0].arguments,
        serde_json::json!({ "amount": 10 }),
        "the executor received the transformed arguments, not the model's"
    );
}

// ---------------------------------------------------------------------------
// The grant itself: a granted dispatch is what lets the happy path run at
// all — every authority layer agreed, and the call executed exactly once.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_fully_authorized_tool_call_executes_exactly_once() {
    let registry = tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::idempotent(2).expect("the spec is valid"),
    );
    let fx = AuthorityFixture::new(tool_then_proposal(), registry, None);
    fx.start().await;
    fx.pump().await;

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}
