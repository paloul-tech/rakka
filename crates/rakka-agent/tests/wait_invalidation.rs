//! Settings changes and credential revocation injected *during* a wait.
//!
//! Slice 6.1's second bullet
//! ([implementation plan](../../docs/plans/rakka-agent/implementation-plan.md)),
//! and the half of scenarios 13 and 53 that had no proof
//! ([specification 7.2, 12.3, and 16](../../docs/plans/rakka-agent/spec.md)).
//!
//! `tool_authority.rs` already proves that a revocation reaches the *next*
//! dispatch — but every one of those tests applies the change before the run
//! has parked, so the run was never waiting when the operator acted. The
//! interesting window is the other one: a run sitting on a durable checkpoint
//! for hours while the world changes underneath it. A revocation that lands
//! there must reach the attempt that the *later* resume produces, and an
//! approval issued after it must not resurrect what the revocation withdrew.
//!
//! Every change here is applied the way an operator applies one — a durable
//! `AgentEntityCommand` against a freshly recovered entity, fenced on the
//! revision it expects to succeed — never by mutating a struct in memory.

mod common;

use common::*;
use rakka_agent::testkit::{sweep_crash_points, DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentApprovalDecision, AgentAuthorityEnvelope, AgentCheckpointDecision,
    AgentCredentialBindingRef, AgentDefinition, AgentDefinitionId, AgentEffectSpec,
    AgentEntityCommand, AgentEntityStore, AgentGuardrail, AgentGuardrailBoundary,
    AgentGuardrailChain, AgentGuardrailContext, AgentGuardrailOutcome, AgentGuardrailStage,
    AgentGuardrailStageId, AgentModelTurn, AgentOperationId, AgentOperationKind, AgentPolicyRef,
    AgentRevisionNumber, AgentRunEntityCommand, AgentRunStatus, AgentSettingsChange,
    AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    AgentToolRegistry, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, PrincipalRef};

const TOOL: &str = "charge-card";

fn tool_id() -> AgentToolId {
    AgentToolId::new(TOOL).expect("tool id should be valid")
}

fn credential() -> AgentCredentialBindingRef {
    AgentCredentialBindingRef::new("payments").expect("the binding ref is valid")
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

fn closing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// One tool turn and one closing proposal, scripted by turn number.
fn tool_then_proposal() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn())
        .with_turn_for(2, closing_turn())
}

/// A world whose gated tool parks the run on an approval checkpoint.
///
/// The optional credential binding is the intent's own: a grant must bind the
/// binding the intent carries, and settings may revoke it out from under both.
async fn parked_world(with_credential: bool) -> AuthorityFixture {
    let mut spec = AgentEffectSpec::non_idempotent();
    if with_credential {
        spec = spec.with_credential_binding(credential());
    }
    let registry = AgentToolRegistry::new()
        .register(tool_binding_for_spec(TOOL, &spec).with_checkpoint_required())
        .expect("the tool registers");
    let mut fx = AuthorityFixture::over(tool_then_proposal(), registry, None);
    if with_credential {
        fx = fx.with_credential_resolver("token-1");
    }
    fx.start().await;
    fx.pump().await;
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForApproval,
        "the run must actually be parked before the change is injected"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    fx
}

/// The open checkpoint the parked run is waiting on.
async fn open_checkpoint(fx: &AuthorityFixture) -> rakka_agent_workflow::HumanCheckpointId {
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
}

/// Approves the parked checkpoint, exactly as a human decision does.
async fn approve(
    fx: &AuthorityFixture,
    checkpoint_id: rakka_agent_workflow::HumanCheckpointId,
    credential_binding: Option<AgentCredentialBindingRef>,
    discriminator: &str,
) {
    let mut store = fx.fx.run();
    store.recover(fx.fx.now()).await.expect("the run recovers");
    store
        .apply(
            AgentRunEntityCommand::ResolveCheckpoint {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::CheckpointResolution,
                    &agent_scope(),
                    discriminator,
                )
                .expect("the decision key derives"),
                checkpoint_id,
                resolver: PrincipalRef {
                    principal_type: "user".to_string(),
                    principal_id: "approver".to_string(),
                    display_name: None,
                },
                decision: Box::new(AgentCheckpointDecision::Approval(
                    AgentApprovalDecision::Approve {
                        credential_binding,
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
}

// ---------------------------------------------------------------------------
// Scenario 13, during the wait: a revocation that lands while the run is parked
// reaches the attempt the later resume produces.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_tool_revoked_during_the_approval_wait_never_dispatches() {
    let fx = parked_world(false).await;
    let checkpoint = open_checkpoint(&fx).await;

    // The operator revokes while the run is waiting. Nothing wakes it: the
    // change is durable on the agent, and the run is still parked.
    fx.apply_settings(
        "revoke-during-wait",
        vec![AgentSettingsChange::RevokeTool(tool_id())],
    )
    .await;
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForApproval,
        "a settings change is not a wake"
    );

    // The approval arrives afterwards, and is genuinely granted — the human had
    // no way to know. The grant is what makes this test worth writing: an
    // authority that consulted the checkpoint before the revocation would
    // dispatch here.
    approve(&fx, checkpoint, None, "d1").await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "tool-revoked");
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        0,
        "an approval issued after a revocation does not resurrect the tool"
    );
}

#[tokio::test]
async fn a_credential_revoked_during_the_approval_wait_never_dispatches() {
    let fx = parked_world(true).await;
    let checkpoint = open_checkpoint(&fx).await;

    fx.apply_settings(
        "revoke-credential-during-wait",
        vec![AgentSettingsChange::RevokeCredentialBinding(credential())],
    )
    .await;
    approve(&fx, checkpoint, Some(credential()), "d1").await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "credential-revoked");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

#[tokio::test]
async fn a_revocation_after_the_approval_still_wins() {
    // The same two events in the other order. Immediate safety is a property of
    // the attempt, not of the decision that preceded it, so the outcome must
    // not depend on which arrived first.
    let fx = parked_world(false).await;
    let checkpoint = open_checkpoint(&fx).await;

    approve(&fx, checkpoint, None, "d1").await;
    fx.apply_settings(
        "revoke-after-approval",
        vec![AgentSettingsChange::RevokeTool(tool_id())],
    )
    .await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "tool-revoked");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// The lifecycle half: an agent suspended during the wait cannot be resumed into
// a dispatch by a decision that arrives afterwards.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_agent_suspended_during_the_approval_wait_defers_the_resumed_attempt() {
    // Suspension is transient, so unlike a revocation it must *defer* rather
    // than fail closed — and it must do so without spending the intent's
    // budget, or a run suspended during its wait could never be resumed.
    // `tool_authority.rs` proves that for a run suspended before it parks;
    // what is new here is that the human decision taken during the suspension
    // survives it intact.
    let fx = parked_world(false).await;
    let checkpoint = open_checkpoint(&fx).await;

    fx.set_suspended(true, "suspend-during-wait").await;
    approve(&fx, checkpoint, None, "d1").await;

    let pass = fx.one_pass().await;
    assert!(pass.claimed >= 1, "the resumed ticket was claimed");
    assert_eq!(pass.invoked, 0, "nothing was invoked under suspension");
    assert!(
        pass.deferred >= 1,
        "the refusal deferred instead of spending"
    );
    assert_eq!(pass.failed_attempts, 0, "no outbox attempt was burned");
    assert!(
        !fx.fx
            .run_snapshot()
            .await
            .expect("the run exists")
            .status
            .is_terminal(),
        "a transient refusal does not fail a parked run"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);

    // Resuming lets the grant taken during the suspension window proceed.
    fx.set_suspended(false, "resume-after-wait").await;
    fx.pump().await;
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed,
        "the approval survived the suspension window"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

// ---------------------------------------------------------------------------
// Specification 12.3: the grant binds the exact intent, including its
// credential binding. A grant issued against a different binding than the
// intent carries is not a grant for that intent.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_approval_bound_to_another_credential_does_not_bind_the_intent() {
    let fx = parked_world(true).await;
    let checkpoint = open_checkpoint(&fx).await;

    // The decision names a *different* credential than the intent carries.
    // An omitted binding would inherit the bound effect's own, which is the
    // ordinary case; naming another one is the approver authorizing something
    // this intent does not do.
    let other = AgentCredentialBindingRef::new("ledger").expect("the binding ref is valid");
    approve(&fx, checkpoint, Some(other), "d1").await;
    fx.pump().await;

    assert_eq!(
        fx.terminal_failure_code().await,
        "checkpoint-grant-credential-changed"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

/// The ordinary case, as the control for the test above: an approval that
/// omits the binding inherits the bound effect's own and dispatches.
#[tokio::test]
async fn an_approval_that_omits_the_binding_inherits_the_intents_own() {
    let fx = parked_world(true).await;
    let checkpoint = open_checkpoint(&fx).await;
    approve(&fx, checkpoint, None, "d1").await;
    fx.pump().await;

    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

// ---------------------------------------------------------------------------
// The third immediate-safety change: a guardrail policy selected while the run
// is parked. Unlike a revocation this withdraws nothing the model can see — it
// says the effect may only run under a policy the deployed chain must
// implement — and a fleet that does not implement it must refuse rather than
// run the effect under the wrong one.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_guardrail_policy_selected_during_the_wait_refuses_an_unimplementing_fleet() {
    let fx = parked_world(false).await;
    let checkpoint = open_checkpoint(&fx).await;

    let policy = AgentPolicyRef::new("strict-payments").expect("the policy ref is valid");
    fx.apply_settings(
        "select-policy-during-wait",
        vec![AgentSettingsChange::GuardrailPolicy(policy)],
    )
    .await;
    approve(&fx, checkpoint, None, "d1").await;
    fx.pump().await;

    assert_eq!(
        fx.terminal_failure_code().await,
        "guardrail-policy-mismatch"
    );
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        0,
        "an effect approved before the policy landed does not run under the old one"
    );
}

// ---------------------------------------------------------------------------
// The rolling-upgrade shape of the same window. The guardrail chain is
// deployment wiring rather than durable agent settings, so an upgrade that
// lands while a run is parked replaces the chain the *dispatcher* evaluates
// while the parked intent stays pinned to the revision it was committed under.
// `tool_authority.rs` proves the pin catches that drift when it exists from the
// start; what a wait adds is that the drift can appear after the human has
// already decided.
// ---------------------------------------------------------------------------

/// A transforming stage, so the chain revision genuinely decides the payload
/// and the pin is load-bearing rather than decorative.
struct ClampAmount;

impl AgentGuardrail for ClampAmount {
    fn evaluate(
        &self,
        _: &AgentGuardrailContext<'_>,
        content: &serde_json::Value,
    ) -> AgentGuardrailOutcome {
        let amount = content.get("amount").and_then(serde_json::Value::as_u64);
        match amount {
            Some(amount) if amount <= 10 => AgentGuardrailOutcome::Allow,
            _ => AgentGuardrailOutcome::Transform {
                content: serde_json::json!({ "amount": 10 }),
                reason_code: "amount-clamped".to_string(),
            },
        }
    }
}

fn clamping_chain(revision: AgentRevisionNumber) -> AgentGuardrailChain {
    AgentGuardrailChain::new(revision)
        .with_stage(
            AgentGuardrailStage::new(
                AgentGuardrailStageId::new("amount-clamp").expect("the stage id is valid"),
                AgentRevisionNumber::INITIAL,
                std::sync::Arc::new(ClampAmount),
            )
            .at_boundary(AgentGuardrailBoundary::ToolRequest),
        )
        .expect("the stage registers")
}

#[tokio::test]
async fn a_guardrail_upgrade_during_the_wait_refuses_the_pinned_intent() {
    let registry = AgentToolRegistry::new()
        .register(
            tool_binding_for_spec(TOOL, &AgentEffectSpec::non_idempotent())
                .with_checkpoint_required(),
        )
        .expect("the tool registers");
    let mut fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(registry.clone())
            .with_guardrails(clamping_chain(AgentRevisionNumber::INITIAL)),
        None,
    );
    fx.start().await;
    fx.pump().await;
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForApproval
    );
    let checkpoint = open_checkpoint(&fx).await;

    // The upgrade lands on the dispatch fleet while the run waits.
    fx.authority = AgentToolAuthority::new(registry)
        .with_guardrails(clamping_chain(AgentRevisionNumber::new(2)));

    approve(&fx, checkpoint, None, "d1").await;
    fx.pump().await;

    assert_eq!(
        fx.terminal_failure_code().await,
        "guardrail-revision-mismatch"
    );
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        0,
        "an intent committed under the old chain does not run under the new one"
    );
}

// ---------------------------------------------------------------------------
// The other two timing classes, for contrast. Only immediate-safety changes may
// reach a decision already taken; a turn-bound change waits for the next turn,
// and a run-pinned one waits for a new run. Neither may disturb a parked wait.
// ---------------------------------------------------------------------------

/// The agent's current settings revision, read from its durable record.
///
/// What a settings change lands on. Reading it back is what separates "the
/// change was applied and the run ignored it" from "the change was never
/// applied" — the two are indistinguishable from the run's status alone.
async fn current_settings(
    fx: &AuthorityFixture,
) -> (AgentRevisionNumber, rakka_agent::AgentSettings) {
    let mut agent = rakka_agent::AgentEntityStore::new(agent_scope(), fx.fx.agents.clone());
    agent.recover().await.expect("the agent recovers");
    let state = agent
        .state()
        .expect("the state reads")
        .expect("the agent exists");
    (
        state.settings().revision(),
        state.settings().settings().clone(),
    )
}

#[tokio::test]
async fn a_turn_bound_change_during_the_wait_does_not_disturb_it() {
    let fx = parked_world(false).await;
    let checkpoint = open_checkpoint(&fx).await;

    let (revision_before, settings_before) = current_settings(&fx).await;
    let pinned_before = fx
        .fx
        .run_snapshot()
        .await
        .expect("the run exists")
        .agent_settings_revision;

    fx.apply_settings(
        "retrieval-limit-during-wait",
        vec![AgentSettingsChange::RetrievalLimit(7)],
    )
    .await;

    // Without this the test cannot tell a turn-bound change that was applied
    // and correctly ignored from an `apply_settings` call that did nothing:
    // deleting the call above left every remaining assertion passing.
    let (revision_after, settings_after) = current_settings(&fx).await;
    assert_ne!(
        revision_after, revision_before,
        "the settings change advanced the agent's settings revision"
    );
    assert_eq!(
        settings_after.retrieval_limit,
        Some(7),
        "the turn-bound change landed on the agent's current settings"
    );
    assert_ne!(
        settings_before.retrieval_limit,
        Some(7),
        "the fixture did not already carry the value under test"
    );
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::WaitingForApproval,
        "a turn-bound change is not a wake and not an invalidation"
    );
    assert_eq!(
        fx.fx
            .run_snapshot()
            .await
            .expect("the run exists")
            .agent_settings_revision,
        pinned_before,
        "a turn-bound change does not re-pin a run that is parked"
    );

    approve(&fx, checkpoint, None, "d1").await;
    fx.pump().await;
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

/// A run-pinned change is inert for a run already in flight.
///
/// The two run-pinned fields — the loop-state and memory schema versions —
/// are stored and resolved by [`rakka_agent::effective_settings_for_turn`] and
/// are read by no dispatch or loop path yet, so the promise "applies only to a
/// new run or an explicit migration" currently holds because nothing consumes
/// them mid-run. This test pins the *behaviour* rather than that implementation
/// fact, so it keeps its meaning when a consumer arrives.
#[tokio::test]
async fn a_run_pinned_change_during_the_wait_is_inert_for_the_live_run() {
    let fx = parked_world(false).await;
    let checkpoint = open_checkpoint(&fx).await;

    let (revision_before, settings_before) = current_settings(&fx).await;
    let pinned_before = fx
        .fx
        .run_snapshot()
        .await
        .expect("the run exists")
        .agent_settings_revision;
    assert_ne!(
        settings_before.loop_state_schema_version,
        Some(rakka_agent_workflow::StateSchemaVersion::new(2)),
        "the fixture did not already carry the value under test"
    );

    fx.apply_settings(
        "schema-bump-during-wait",
        vec![AgentSettingsChange::LoopStateSchemaVersion(
            rakka_agent_workflow::StateSchemaVersion::new(2),
        )],
    )
    .await;
    approve(&fx, checkpoint, None, "d1").await;
    fx.pump().await;

    // The change landed on the agent...
    let (revision_after, settings_after) = current_settings(&fx).await;
    assert_ne!(
        revision_after, revision_before,
        "the settings change advanced the agent's settings revision"
    );
    assert_eq!(
        settings_after.loop_state_schema_version,
        Some(rakka_agent_workflow::StateSchemaVersion::new(2)),
        "the run-pinned change landed on the agent's current settings"
    );

    // ...and the run in flight stayed on the revision it pinned. This is the
    // assertion that keeps its meaning when a consumer of
    // `loop_state_schema_version` arrives: status and invocation count are
    // identical to the no-change control and cannot distinguish a run that
    // stayed pinned from one that silently migrated.
    assert_eq!(
        fx.fx
            .run_snapshot()
            .await
            .expect("the run exists")
            .agent_settings_revision,
        pinned_before,
        "a run-pinned change does not re-pin a run that is already in flight"
    );
    assert_eq!(
        fx.fx.run_snapshot().await.expect("the run exists").status,
        AgentRunStatus::Completed,
        "a run-pinned change does not migrate a run that is already in flight"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
}

// ---------------------------------------------------------------------------
// The window and the loss, together. A revocation is only useful if it also
// survives the owner dying between the operator's command and the resumed
// attempt — the two events this slice exists to interleave.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_revocation_during_the_wait_survives_every_owner_loss() {
    // The reference flow, to count the writes the swept flow will make.
    let writes = {
        let fx = parked_world(false).await;
        let checkpoint = open_checkpoint(&fx).await;
        fx.apply_settings(
            "revoke-during-wait",
            vec![AgentSettingsChange::RevokeTool(tool_id())],
        )
        .await;
        approve(&fx, checkpoint, None, "d1").await;
        fx.fx.runs.reset_writes();
        fx.pump().await;
        assert_eq!(fx.terminal_failure_code().await, "tool-revoked");
        fx.fx.runs.writes()
    };
    assert!(
        writes >= 1,
        "the resumed attempt writes the run store at least once, saw {writes}"
    );

    sweep_crash_points(writes, |nth, point| async move {
        let fx = parked_world(false).await;
        let checkpoint = open_checkpoint(&fx).await;
        fx.apply_settings(
            "revoke-during-wait",
            vec![AgentSettingsChange::RevokeTool(tool_id())],
        )
        .await;
        approve(&fx, checkpoint, None, "d1").await;

        fx.fx.runs.reset_writes();
        fx.fx.runs.crash_at(nth, point);
        let _ = fx.try_pump().await;
        fx.fx.runs.assert_crash_fired(nth, point);
        fx.fx.runs.survive();
        // The dead pass's fleet lease lapses before its work is re-claimable —
        // a lease is a fence, never proof of what the dead worker did.
        fx.expire_lease();

        // A new owner, with nothing but the durable record — and the
        // revocation is part of that record.
        fx.try_pump().await.unwrap_or_else(|error| {
            panic!("crash {point:?} at write {nth} did not converge: {error}")
        });
        assert_eq!(
            fx.terminal_failure_code().await,
            "tool-revoked",
            "crash {point:?} at write {nth}: the revocation outlived the owner"
        );
        assert_eq!(
            fx.tools.invocation_count(TOOL),
            0,
            "crash {point:?} at write {nth}: nothing was invoked"
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// The task-level wait. A run-level wait parks a run that already exists; a
// dependency block is the other kind — the task waits with no run at all, and
// the assignment decision that ends the wait is taken later, against whatever
// definition is in force *then*. `autonomy_admission.rs` proves that derivation
// for a task created after the change; what a wait adds is that the change can
// land in the gap between the task's creation and its assignment.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_definition_narrowed_while_a_task_is_blocked_refuses_its_later_assignment() {
    let fx = Fixture::new(ScriptedDispatcher::new().with_turn(closing_turn()));
    fx.instantiate_agent().await;

    // A task blocked on a dependency: it exists, it is durable, and it has no
    // run yet. The assignment decision has not been taken.
    let blocker = rakka_agent::AgentTaskId::new("blocker-1").expect("the task id is valid");
    fx.apply_task_command(rakka_agent::AgentTaskEntityCommand::Create {
        operation_id: AgentOperationId::new(
            AgentOperationKind::TaskCreation,
            [TENANT, task_scope().task().as_str(), "1"],
        )
        .expect("the operation id derives"),
        creation: Box::new(rakka_agent::AgentTaskCreation {
            definition: task_definition(),
            input: AgentTaskContent::inline(serde_json::json!({ "ticket": 1 }))
                .expect("the input is inline-bounded"),
            assignee: Some(agent_id()),
            team: None,
            goal: None,
            goal_mode: Default::default(),
            goal_spec: None,
            parent: None,
            dependencies: vec![rakka_agent::AgentTaskDependencyDeclaration::new(
                blocker.clone(),
            )],
            escrow: None,
            wake: None,
            telemetry: Default::default(),
            delegation: None,
        }),
    })
    .await
    .expect("the blocked task creates");
    assert_eq!(
        fx.task_snapshot().await.status,
        rakka_agent::AgentTaskStatus::Blocked,
        "the task must actually be waiting before the change is injected"
    );

    // The operator republishes a definition that no longer declares this task
    // definition. Nothing wakes the task; the change is simply durable.
    let mut narrowed = AgentAuthorityEnvelope::empty();
    narrowed.task_definitions.insert(
        rakka_agent::AgentTaskDefinitionId::new("something-else")
            .expect("the definition id is valid"),
    );
    let definition = AgentDefinition::new(
        AgentDefinitionId::new("narrowed-v1").expect("definition id should be valid"),
        "No longer resolves tickets.",
        narrowed,
    )
    .expect("the agent definition should be valid");
    let mut agent = AgentEntityStore::new(agent_scope(), fx.agents.clone());
    agent.recover().await.expect("the agent should recover");
    agent
        .apply(AgentEntityCommand::PublishDefinition {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::DefinitionUpdate,
                &agent_scope(),
                "narrow",
            )
            .expect("operation id should be derivable"),
            definition: Box::new(definition),
            provenance: Box::new(provenance(3)),
        })
        .await
        .expect("the narrowing definition publishes");

    // The dependency resolves, and the assignment decision is taken now.
    fx.apply_task_command(
        rakka_agent::AgentTaskEntityCommand::RecordDependencyOutcome {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Command,
                [TENANT, task_scope().task().as_str(), "blocker-done"],
            )
            .expect("the operation id derives"),
            dependency: blocker,
            outcome: rakka_agent::AgentTaskDependencyOutcome::Completed,
        },
    )
    .await
    .expect("the dependency outcome records");
    let _ = fx.pump().await;

    let snapshot = fx.task_snapshot().await;
    assert!(
        fx.run_snapshot().await.is_none(),
        "no run may be created for work the current definition does not declare"
    );
    assert_eq!(
        snapshot.last_refusal.expect("a refusal is recorded").reason,
        rakka_agent::AgentAssignmentRefusalReason::TaskDefinitionNotPermitted
    );
    assert!(
        !snapshot.status.is_terminal(),
        "the task stays assignable rather than failing: the remedy is an operator's, \
         and a task that failed itself could not take it"
    );
}
