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
    sweep_crash_points, DeterministicModelAdapter, InProcessRunResultDelivery, KillSwitchProbe,
    RecordingToolExecutor, ScriptedDispatcher, SharedAtomicWorkflowClock,
};
use rakka_agent::{
    AgentAuthorityEnvelope, AgentDefinition, AgentDefinitionId, AgentDefinitionRevision,
    AgentDispatchAuthority, AgentDispatchDecision, AgentDispatchFuture, AgentDispatchPass,
    AgentDispatchWindow, AgentEffectSpec, AgentEntityAuthority, AgentEntityCommand,
    AgentEntityStore, AgentExecutionPolicyRef, AgentExecutionPolicyRouter, AgentGuardrail,
    AgentGuardrailBoundary, AgentGuardrailChain, AgentGuardrailContext, AgentGuardrailOutcome,
    AgentGuardrailStage, AgentGuardrailStageId, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentPolicyRef, AgentReconciliationProtocolRef, AgentRevisionNumber,
    AgentRunEffect, AgentRunEffectDispatcher, AgentRunEffectStatus, AgentRunScope, AgentRunState,
    AgentRunStatus, AgentRunTerminalReason, AgentSettings, AgentSettingsChange, AgentSetupRevision,
    AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest,
    AgentToolDeclaration, AgentToolId, AgentToolRegistry, WorkflowAgentRunEffectSink,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::WorkflowState;
use rakka_agent_workflow::AgentTimestampMillis;
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

/// An [`AgentDispatchAuthority`] decorator that backdates every issued
/// grant's expiry, so the dispatcher's own pre-attempt revalidation — not the
/// authority — is what refuses the attempt.
struct ExpiredGrantAuthority<Inner>(Inner);

impl<Inner: AgentDispatchAuthority> AgentDispatchAuthority for ExpiredGrantAuthority<Inner> {
    fn authorize<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        run: &'a AgentRunState,
        intent: &'a AgentRunEffect,
        attempt: u32,
        now: AgentTimestampMillis,
    ) -> AgentDispatchFuture<'a, AgentDispatchDecision> {
        let inner = self.0.authorize(scope, run, intent, attempt, now);
        Box::pin(async move {
            match inner.await? {
                AgentDispatchDecision::Granted(mut granted) => {
                    granted.grant.expires_at =
                        AgentTimestampMillis::new(now.as_millis().saturating_sub(1));
                    Ok(AgentDispatchDecision::Granted(granted))
                }
                refused => Ok(refused),
            }
        })
    }
}

/// The authority fixture: the common task-and-run fixture over the durable
/// workflow-outbox sink, plus the fleet, the executor, the kill-switch probe,
/// and a configurable [`AgentToolAuthority`] behind the pipeline's required
/// gate.
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
    probe: KillSwitchProbe,
    expire_grants: bool,
}

impl AuthorityFixture {
    /// A fixture whose commit-time policies are the given authority's own
    /// projection ([`AgentToolAuthority::effect_policies`]), so the intents
    /// the run commits carry the guardrail-revision pin the dispatch gate
    /// validates.
    fn new(
        adapter: DeterministicModelAdapter,
        authority: AgentToolAuthority,
        model_spec: Option<AgentEffectSpec>,
    ) -> Self {
        let registry = authority.registry().clone();
        let mut policies = authority
            .effect_policies()
            .expect("the authority projects valid policies");
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
            probe: KillSwitchProbe::new(),
            expire_grants: false,
        }
    }

    /// A fixture over the plain authority of one registry.
    fn over(
        adapter: DeterministicModelAdapter,
        registry: AgentToolRegistry,
        model_spec: Option<AgentEffectSpec>,
    ) -> Self {
        Self::new(adapter, AgentToolAuthority::new(registry), model_spec)
    }

    /// Replaces the dispatch-gate authority *without* recomputing the
    /// commit-time policies — the deliberate commit/dispatch drift the
    /// guardrail-revision pin must catch.
    fn with_gate_authority(mut self, authority: AgentToolAuthority) -> Self {
        self.authority = authority;
        self
    }

    /// Backdates every issued grant's expiry, so the dispatcher's
    /// pre-attempt revalidation refuses it.
    fn with_expired_grants(mut self) -> Self {
        self.expire_grants = true;
        self
    }

    /// Replaces the envelope the agent is instantiated under.
    fn with_envelope(mut self, envelope: AgentAuthorityEnvelope) -> Self {
        self.envelope = envelope;
        self
    }

    /// Enforces the given run setup at dispatch, for this fixture's run.
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
            gate = gate.with_setup_for_run(run_scope(), setup.clone());
        }
        let gate: Arc<dyn AgentDispatchAuthority> = if self.expire_grants {
            Arc::new(ExpiredGrantAuthority(gate))
        } else {
            Arc::new(gate)
        };
        AgentRunEffectDispatcher::new(
            AgentDispatcherWorkerId::new("worker-1"),
            self.workflow_store.clone(),
            self.fleet_store.clone(),
            self.fx.runs.clone(),
            self.wf_clock.clone(),
            Arc::new(self.adapter.clone()),
            Arc::new(self.tools.clone()),
            gate,
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
    }

    /// Advances the shared clock past the fleet lease.
    fn expire_lease(&self) {
        self.wf_clock.advance(LEASE_MS + 1);
    }

    /// The durable status of the effect at one slot of the run's loop state.
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

    /// Drives the run until its tool ticket is flushed and ready to claim.
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
    let fx = AuthorityFixture::over(tool_then_proposal(), AgentToolRegistry::new(), None)
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
    let fx = AuthorityFixture::over(tool_then_proposal(), registry, None).with_envelope(envelope);
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
    let fx = AuthorityFixture::over(tool_then_proposal(), registry, None);

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
    let fx = AuthorityFixture::over(tool_then_proposal(), registry, None);
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
// Scenario 54 / spec 12: the checkpoint gate. A binding that requires an
// effect-bound checkpoint grant is undispatchable without one — since slice
// 1.10's run-side wave, the run parks on an approval checkpoint rather than
// letting the effect reach the dispatcher and fail. Either way the tool is not
// invoked until a decision issues a grant.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_checkpoint_requiring_tool_parks_for_approval_and_is_not_invoked() {
    let registry = AgentToolRegistry::new()
        .register(
            tool_binding_for_spec(TOOL, &AgentEffectSpec::non_idempotent())
                .with_checkpoint_required(),
        )
        .expect("the tool registers");
    let fx = AuthorityFixture::over(tool_then_proposal(), registry, None);
    fx.start().await;
    fx.pump().await;

    // The run reaches its checkpoint wait and quiesces there — passivated, no
    // live task — rather than becoming terminal, and the tool never runs.
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::WaitingForApproval);
    assert!(!run.status.is_terminal());
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

#[tokio::test]
async fn a_resolved_checkpoint_grant_lets_the_gated_tool_dispatch() {
    // The whole loop: the run parks, an approval stores the digest-bound grant,
    // and the *real* dispatch authority — not a stub — sources that grant,
    // revalidates it against the exact intent, and lets the tool execute.
    let registry = AgentToolRegistry::new()
        .register(
            tool_binding_for_spec(TOOL, &AgentEffectSpec::non_idempotent())
                .with_checkpoint_required(),
        )
        .expect("the tool registers");
    let fx = AuthorityFixture::over(tool_then_proposal(), registry, None);
    fx.start().await;
    fx.pump().await;

    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForApproval
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);

    let checkpoint_id = {
        let mut store = fx.fx.run();
        store.recover(fx.fx.now()).await.expect("the run recovers");
        store
            .state()
            .expect("state reads")
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()[0]
            .checkpoint_id
            .clone()
    };

    let mut store = fx.fx.run();
    store.recover(fx.fx.now()).await.expect("the run recovers");
    store
        .apply(
            rakka_agent::AgentRunEntityCommand::ResolveCheckpoint {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::CheckpointResolution,
                    &agent_scope(),
                    "d1",
                )
                .expect("the decision key derives"),
                checkpoint_id,
                resolver: rakka_agent_workflow::PrincipalRef {
                    principal_type: "user".to_string(),
                    principal_id: "approver".to_string(),
                    display_name: None,
                },
                decision: Box::new(rakka_agent::AgentCheckpointDecision::Approval(
                    rakka_agent::AgentApprovalDecision::Approve {
                        credential_binding: None,
                        expires_at: AgentTimestampMillis::new(1_000_000),
                        allowed_use_count: 1,
                    },
                )),
            },
            &fx.fx.router,
            fx.fx.now(),
        )
        .await
        .expect("the approval applies");

    fx.pump().await;

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
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
    let fx = AuthorityFixture::over(tool_then_proposal(), registry.clone(), None);
    fx.start().await;
    fx.pump().await;
    assert_eq!(
        fx.terminal_failure_code().await,
        "execution-policy-unroutable"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);

    // A router that accepts the class routes the same intent to execution.
    let routed = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(registry).with_execution_router(Arc::new(AcceptClass("sandboxed"))),
        None,
    );
    routed.start().await;
    routed.pump().await;
    let run = routed.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(routed.tools.invocation_count(TOOL), 1);
}

// ---------------------------------------------------------------------------
// Scenario 54: grant validity. A grant whose window has already closed is
// rechecked — and refused — before the attempt, by the dispatcher itself.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_expired_grant_is_undispatchable() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let fx = AuthorityFixture::over(tool_then_proposal(), registry, None).with_expired_grants();
    fx.start().await;
    fx.pump().await;

    // The model call is the first dispatch whose backdated grant the
    // dispatcher's pre-attempt revalidation refuses; nothing external was
    // ever invoked.
    assert_eq!(fx.terminal_failure_code().await, "grant-expired");
    assert_eq!(fx.adapter.calls(), 0);
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// The grant TTL knob: a grant minted and spent at the same instant — the
// per-attempt derive path — is valid whatever the TTL, including zero. TTL
// enforcement over elapsed time is the unit-level validate_for contract; it
// must never refuse a grant at its own issuing instant.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_zero_ttl_grant_still_covers_its_issuing_instant() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(registry).with_grant_ttl_ms(0),
        None,
    );
    fx.start().await;
    fx.pump().await;

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

// ---------------------------------------------------------------------------
// Scenario 54 / 53: the immediate-safety suspension. A suspended agent
// dispatches nothing — the refusal defers the ticket without spending the
// intent's budget, so resuming lets the very next attempt proceed even under
// the fail-safe single-attempt defaults.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_suspended_agent_dispatches_nothing_until_resumed() {
    // Deliberately the shipped defaults: the model effect permits exactly one
    // attempt. If suspension spent budget, this run could never recover.
    let adapter = DeterministicModelAdapter::new().with_turn(proposing_turn("resolved"));
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let fx = AuthorityFixture::over(adapter, registry, None);
    fx.start().await;
    fx.set_suspended(true, "suspend-1").await;

    // One pass under suspension: the ticket is claimed, the authority refuses
    // before durable `Started`, nothing is invoked, and nothing durable is
    // spent — the claim is only deferred at the fleet.
    let pass = fx.one_pass().await;
    assert!(pass.claimed >= 1, "the ticket was claimed");
    assert_eq!(pass.invoked, 0, "nothing was invoked under suspension");
    assert!(
        pass.deferred >= 1,
        "the refusal deferred instead of spending"
    );
    assert_eq!(pass.failed_attempts, 0, "no outbox attempt was burned");
    assert_eq!(fx.adapter.calls(), 0);
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert!(
        !run.status.is_terminal(),
        "a transient refusal does not fail the run"
    );

    // Resuming clears the condition; the next attempt rechecks and proceeds
    // on the untouched single-attempt budget.
    fx.set_suspended(false, "resume-1").await;
    fx.pump().await;
    assert_eq!(fx.adapter.calls(), 1, "exactly one model invocation");
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

// ---------------------------------------------------------------------------
// A suspension while a reconcileable ticket is due must not be misread by
// recovery as a possibly-executed attempt: nothing durable is written, so
// after resume the effect completes without any reconciliation — with no
// reconciler wired, a misclassification would park the run indeterminate.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_suspended_reconcileable_effect_is_not_misread_as_ambiguous() {
    let protocol = AgentReconciliationProtocolRef::new("payment-ledger").expect("the ref is valid");
    let registry = tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::reconcileable(protocol, 3).expect("the spec is valid"),
    );
    let fx = AuthorityFixture::over(tool_then_proposal(), registry, None);
    fx.start().await;

    // Turn one runs; the tool ticket reaches the outbox. Then the agent is
    // suspended before any tool attempt.
    fx.pump_until_tool_ticket().await;
    fx.set_suspended(true, "suspend-1").await;
    let pass = fx.one_pass().await;
    assert!(pass.deferred >= 1, "the tool ticket was deferred");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);

    // Resume: the ticket dispatches as a *fresh* first attempt. No reconciler
    // is configured, so completing proves recovery never classified the
    // deferred ticket as a possibly-executed ambiguity.
    fx.set_suspended(false, "resume-1").await;
    fx.pump().await;
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

// ---------------------------------------------------------------------------
// Specification 11.5: an undispatchable verdict on the recovery retry of an
// ambiguous idempotent loss parks the generation indeterminate — the prior
// attempt may have committed externally, so "failed, nothing invoked" would
// erase exactly the ambiguity an operator must resolve.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_undispatchable_recovery_retry_parks_the_ambiguity() {
    let registry = tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::idempotent(3).expect("the spec is valid"),
    );
    let fx = AuthorityFixture::over(tool_then_proposal(), registry, None);
    fx.start().await;

    // The tool attempt invokes the target — the external system commits —
    // and the worker dies before any receipt is recorded: ambiguous.
    fx.pump_until_tool_ticket().await;
    fx.probe.arm(AgentDispatchWindow::AfterInvocation);
    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the pass runs");
    assert!(pass.died);
    assert_eq!(fx.tools.invocation_count(TOOL), 1, "the target committed");

    // The tool is revoked before recovery, so the truth-finding retry is
    // refused definitively.
    fx.apply_settings(
        "revoke-tool",
        vec![AgentSettingsChange::RevokeTool(tool_id())],
    )
    .await;
    fx.expire_lease();
    fx.pump().await;

    // The generation parks indeterminate under the refusal's code instead of
    // failing as if nothing was invoked, and the run awaits the explicit
    // reconciliation decision.
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::WaitingForReconciliation);
    assert_eq!(
        fx.effect_status(1).await,
        Some(AgentRunEffectStatus::Indeterminate),
        "the ambiguity is preserved, not rewritten as a clean failure"
    );
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "the refused retry never re-invoked the target"
    );
}

// ---------------------------------------------------------------------------
// Specification 7.2: the guardrail-policy selection is an immediate-safety
// settings field. A selection the deployed chain does not implement refuses
// dispatch on the very next attempt; a chain that carries the selected
// policy reference proceeds.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_guardrail_policy_selection_is_enforced_at_dispatch() {
    let policy = AgentPolicyRef::new("pii-v2").expect("the policy ref is valid");

    // The deployed chain does not implement the selected policy: refused.
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let fx = AuthorityFixture::over(tool_then_proposal(), registry.clone(), None);
    fx.start().await;
    fx.apply_settings(
        "select-guardrail-policy",
        vec![AgentSettingsChange::GuardrailPolicy(policy.clone())],
    )
    .await;
    fx.pump().await;
    assert_eq!(
        fx.terminal_failure_code().await,
        "guardrail-policy-mismatch"
    );
    assert_eq!(fx.adapter.calls(), 0);
    assert_eq!(fx.tools.invocation_count(TOOL), 0);

    // A chain labeled with the selected policy implements it: the same
    // selection dispatches.
    let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
        .with_policy_ref(policy.clone())
        .with_stage(
            AgentGuardrailStage::new(
                stage_id("allow-all"),
                AgentRevisionNumber::INITIAL,
                Arc::new(AllowAll),
            )
            .at_boundary(AgentGuardrailBoundary::ToolRequest),
        )
        .expect("the stage registers");
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(registry).with_guardrails(chain),
        None,
    );
    fx.start().await;
    fx.apply_settings(
        "select-guardrail-policy",
        vec![AgentSettingsChange::GuardrailPolicy(policy)],
    )
    .await;
    fx.pump().await;
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

// ---------------------------------------------------------------------------
// Specification 16: the guardrail-revision pin. An intent committed without
// the pin — or under a different chain revision — is refused whenever a
// transform decides the payload, so one external idempotency key can never
// carry two differently transformed payloads across a chain change.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_transforming_chain_refuses_an_intent_pinned_to_another_revision() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let transforming = |revision: AgentRevisionNumber| {
        AgentGuardrailChain::new(revision)
            .with_stage(
                AgentGuardrailStage::new(
                    stage_id("amount-clamp"),
                    AgentRevisionNumber::INITIAL,
                    Arc::new(ClampAmount),
                )
                .at_boundary(AgentGuardrailBoundary::ToolRequest),
            )
            .expect("the stage registers")
    };

    // Policies are pinned to chain revision 1; the dispatch gate evaluates
    // revision 2 — the rolling-upgrade drift the pin exists to catch.
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(registry.clone())
            .with_guardrails(transforming(AgentRevisionNumber::INITIAL)),
        None,
    )
    .with_gate_authority(
        AgentToolAuthority::new(registry.clone())
            .with_guardrails(transforming(AgentRevisionNumber::new(2))),
    );
    fx.start().await;
    fx.pump().await;
    assert_eq!(
        fx.terminal_failure_code().await,
        "guardrail-revision-mismatch"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);

    // An intent committed with no pin at all is refused the same way when a
    // transform would decide the payload.
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(registry.clone()),
        None,
    )
    .with_gate_authority(
        AgentToolAuthority::new(registry)
            .with_guardrails(transforming(AgentRevisionNumber::new(2))),
    );
    fx.start().await;
    fx.pump().await;
    assert_eq!(
        fx.terminal_failure_code().await,
        "guardrail-revision-mismatch"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

/// A stage that allows everything, for chains whose identity is the point.
struct AllowAll;

impl AgentGuardrail for AllowAll {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, _: &Value) -> AgentGuardrailOutcome {
        AgentGuardrailOutcome::Allow
    }
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
    let fx = AuthorityFixture::over(tool_then_proposal(), registry, None).with_envelope(envelope);
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "guardrail-stage-missing");
    assert_eq!(fx.adapter.calls(), 0, "the model boundary is guarded too");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// Scenario 44 / specification 16: presence is not coverage. A mandatory stage
// that *is* in the deployment's chain but runs only at a boundary this slice
// has no evaluation point for would satisfy the envelope while protecting
// nothing — the exact fail-open the boundary-less-stage refusal closes at
// registration, and the only place it can be caught is where the evaluated
// boundaries are known.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_mandatory_stage_bound_to_an_unevaluated_boundary_fails_closed() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let mut envelope = envelope_for_registry(&registry);
    envelope.mandatory_guardrails.insert(stage_id("pii-filter"));

    // The stage is real, mandatory, and present — but bound only to the
    // tool-*response* boundary, which slice 1.8 never evaluates.
    let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
        .with_stage(
            AgentGuardrailStage::new(
                stage_id("pii-filter"),
                AgentRevisionNumber::INITIAL,
                Arc::new(AllowAll),
            )
            .at_boundary(AgentGuardrailBoundary::ToolResponse)
            .mandatory(),
        )
        .expect("the stage registers");
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(registry).with_guardrails(chain),
        None,
    )
    .with_envelope(envelope);
    fx.start().await;
    fx.pump().await;

    assert_eq!(
        fx.terminal_failure_code().await,
        "guardrail-stage-unevaluated"
    );
    assert_eq!(fx.adapter.calls(), 0);
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

    let fx = AuthorityFixture::over(tool_then_proposal(), registry, None)
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

/// Blocks over-limit amounts *for one named tool* — the per-tool policy the
/// context's tool identity is what makes expressible. A stage handed only the
/// arguments could not tell `charge-card` from any other call carrying an
/// `amount`.
struct BlockLargeAmounts(AgentToolId);

impl AgentGuardrail for BlockLargeAmounts {
    fn evaluate(
        &self,
        context: &AgentGuardrailContext<'_>,
        content: &Value,
    ) -> AgentGuardrailOutcome {
        if context.tool != Some(&self.0) {
            return AgentGuardrailOutcome::Allow;
        }
        // Fail closed on anything that does not parse as the expected u64: a
        // float, a negative, or an out-of-range amount must never sail past
        // the limit by collapsing to a harmless default.
        let amount = content.get("amount").and_then(Value::as_u64);
        match amount {
            Some(amount) if amount <= 10 => AgentGuardrailOutcome::Allow,
            _ => AgentGuardrailOutcome::Block {
                reason_code: "amount-over-limit".to_string(),
                evidence: None,
            },
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
                Arc::new(BlockLargeAmounts(tool_id())),
            )
            .at_boundary(AgentGuardrailBoundary::ToolRequest)
            .mandatory(),
        )
        .expect("the stage registers");
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(registry).with_guardrails(chain.clone()),
        None,
    );
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "guardrail-blocked");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);

    // The deployment-mandatory stage cannot be narrowed away by any
    // definition or setup: the removal operation itself refuses.
    let error = chain
        .narrowed(
            &BTreeSet::from([stage_id("amount-limit")]),
            AgentRevisionNumber::new(2),
        )
        .expect_err("a mandatory stage cannot be removed");
    assert_eq!(error.code(), "guardrail-mandatory-stage-immutable");
}

// ---------------------------------------------------------------------------
// Specification 16: the evaluation context names the tool, so a stage can
// carry per-tool policy. The same stage, the same over-limit arguments, and
// the same chain — scoped to a *different* tool — allows the call through,
// which is what proves the tool identity reaching the stage is real and
// load-bearing rather than merely carried.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_guardrail_stage_gates_the_tool_the_context_names() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let other = AgentToolId::new("refund-card").expect("tool id should be valid");
    let chain = AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
        .with_stage(
            AgentGuardrailStage::new(
                stage_id("amount-limit"),
                AgentRevisionNumber::INITIAL,
                // Scoped to a tool this run never calls: the amount is over
                // the limit, but this stage is not the one gating `TOOL`.
                Arc::new(BlockLargeAmounts(other)),
            )
            .at_boundary(AgentGuardrailBoundary::ToolRequest)
            .mandatory(),
        )
        .expect("the stage registers");
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(registry).with_guardrails(chain),
        None,
    );
    fx.start().await;
    fx.pump().await;

    // The identical arguments that `a_guardrail_block_keeps_a_tool_call_undispatchable`
    // blocks reach the executor here, because the stage looked at the tool.
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    let invocations = fx.tools.invocations();
    assert_eq!(
        invocations[0].arguments,
        serde_json::json!({ "amount": 42 }),
        "the untransformed arguments reached the target"
    );
}

// ---------------------------------------------------------------------------
// Specification 16: a deterministic guardrail transform replaces the
// arguments the executor sees, and the durable intent is untouched — every
// retry re-derives the identical transformed input.
// ---------------------------------------------------------------------------

struct ClampAmount;

impl AgentGuardrail for ClampAmount {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        // Fail closed on anything that does not parse as the expected u64:
        // an unparseable amount clamps rather than passing unmodified.
        let amount = content.get("amount").and_then(Value::as_u64);
        match amount {
            Some(amount) if amount <= 10 => AgentGuardrailOutcome::Allow,
            _ => AgentGuardrailOutcome::Transform {
                content: serde_json::json!({ "amount": 10 }),
                reason_code: "amount-clamped".to_string(),
            },
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
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(registry).with_guardrails(chain),
        None,
    );
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
    let fx = AuthorityFixture::over(tool_then_proposal(), registry, None);
    fx.start().await;
    fx.pump().await;

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

impl AuthorityFixture {
    /// [`Self::settle`], but surfacing the first error instead of panicking —
    /// what a sweep needs, because an armed crash point kills the run's owner
    /// mid-settle and the injected loss is the point, not a failure.
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
    /// under the same sweep contract as [`Self::try_settle`].
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
                self.try_settle().await?;
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
}

#[tokio::test]
async fn a_fully_authorized_tool_call_executes_once_under_any_owner_loss() {
    // Scenario 54's positive edge under the owner-kill sweep: kill the run's
    // owner at every durable write of the fully-authorized flow. However the
    // owner died, the converged run completed, and the external system saw
    // exactly one idempotency key — a delivery lost after the external commit
    // may legitimately re-invoke the idempotent tool, but only ever under the
    // key the first attempt minted (scenario 7's contract, met here by the
    // run-owner loss rather than dispatcher loss). The model policy is
    // declared idempotent so an ambiguous model window retries instead of
    // failing closed — the non-idempotent model semantics are scenario 9's,
    // proven in `effect_dispatch.rs`.
    let build = || {
        let adapter = tool_then_proposal()
            .with_retry_policy(rakka_agent::AgentModelRetryPolicy {
                safety_class: rakka_agent::AgentEffectSafetyClass::Idempotent,
                max_attempts: 3,
            })
            .expect("the adapter policy is valid");
        let registry = tool_registry_for_spec(
            TOOL,
            &AgentEffectSpec::idempotent(2).expect("the spec is valid"),
        );
        AuthorityFixture::over(
            adapter,
            registry,
            Some(AgentEffectSpec::idempotent(3).expect("the model spec is valid")),
        )
    };
    let reference = build();
    reference
        .fx
        .instantiate_agent_with_envelope(reference.envelope.clone())
        .await;
    reference.fx.runs.reset_writes();
    reference.fx.create_task().await;
    reference
        .try_pump()
        .await
        .expect("the reference flow completes");
    let writes = reference.fx.runs.writes();
    assert!(
        writes >= 6,
        "the authorized flow should make several durable writes, saw {writes}"
    );

    sweep_crash_points(writes, |nth, point| async move {
        let fx = build();
        fx.fx
            .instantiate_agent_with_envelope(fx.envelope.clone())
            .await;

        fx.fx.runs.crash_at(nth, point);
        fx.fx.create_task().await;
        let _crashed = fx.try_pump().await;

        // A new owner activates and finds only what was durably committed; the
        // dead pass's fleet lease lapses before its work is re-claimable.
        fx.fx.runs.assert_crash_fired(nth, point);
        fx.fx.runs.survive();
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

        let keys: BTreeSet<String> = fx
            .tools
            .invocations()
            .into_iter()
            .filter(|invocation| invocation.tool == TOOL)
            .map(|invocation| invocation.idempotency_key)
            .collect();
        assert_eq!(
            keys.len(),
            1,
            "crash {point:?} at write {nth} invoked under a second idempotency key"
        );
    })
    .await;
}

#[tokio::test]
async fn a_pre_crash_revocation_blocks_dispatch_after_any_owner_loss() {
    // Scenario 13 under the owner-kill sweep: the revocation committed before
    // the crash, so no recovery path — whatever write the owner died at — may
    // make the revoked tool dispatchable again. Zero invocations is what
    // "immediate revocation" means, and the durable failure names the check.
    // The model policy is declared idempotent for the same reason as the
    // authorized sweep above: an ambiguous model window must retry so every
    // window converges on the *tool's* refusal.
    let build = || {
        let adapter = tool_then_proposal()
            .with_retry_policy(rakka_agent::AgentModelRetryPolicy {
                safety_class: rakka_agent::AgentEffectSafetyClass::Idempotent,
                max_attempts: 3,
            })
            .expect("the adapter policy is valid");
        let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
        AuthorityFixture::over(
            adapter,
            registry,
            Some(AgentEffectSpec::idempotent(3).expect("the model spec is valid")),
        )
    };
    let reference = build();
    reference
        .fx
        .instantiate_agent_with_envelope(reference.envelope.clone())
        .await;
    reference
        .apply_settings(
            "revoke-tool",
            vec![AgentSettingsChange::RevokeTool(tool_id())],
        )
        .await;
    reference.fx.runs.reset_writes();
    reference.fx.create_task().await;
    reference
        .try_pump()
        .await
        .expect("the reference flow converges");
    assert_eq!(reference.terminal_failure_code().await, "tool-revoked");
    let writes = reference.fx.runs.writes();
    assert!(
        writes >= 4,
        "the revoked flow should make several durable writes, saw {writes}"
    );

    sweep_crash_points(writes, |nth, point| async move {
        let fx = build();
        fx.fx
            .instantiate_agent_with_envelope(fx.envelope.clone())
            .await;
        fx.apply_settings(
            "revoke-tool",
            vec![AgentSettingsChange::RevokeTool(tool_id())],
        )
        .await;

        fx.fx.runs.crash_at(nth, point);
        fx.fx.create_task().await;
        let _crashed = fx.try_pump().await;

        fx.fx.runs.assert_crash_fired(nth, point);
        fx.fx.runs.survive();
        fx.expire_lease();
        fx.try_pump().await.unwrap_or_else(|error| {
            panic!("crash {point:?} at write {nth} did not converge: {error}")
        });

        assert_eq!(
            fx.terminal_failure_code().await,
            "tool-revoked",
            "crash {point:?} at write {nth} lost the revocation refusal"
        );
        assert_eq!(
            fx.tools.invocation_count(TOOL),
            0,
            "crash {point:?} at write {nth} let a revoked tool dispatch"
        );
    })
    .await;
}
