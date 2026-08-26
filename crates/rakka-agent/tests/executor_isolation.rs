//! Trust-class routing: a ticket reaches a worker that serves its class.
//!
//! Specification: sections 11.8 (routing by bounded trust/execution class,
//! and the deployments that may not claim isolation) and 16; scenario 54's
//! routing half.
//!
//! Why `tool_authority.rs`'s two execution-policy tests were not enough: they
//! run **one** router on **one** worker. They prove the gate refuses a class
//! the worker does not accept, which is correct — and they say nothing about
//! which worker gets the ticket, because there is only one. What they
//! actually demonstrate, read carefully, is the defect: a general worker that
//! meets a sandboxed ticket refuses it *definitively*
//! (`AgentAuthorityRefusal::of` is not retryable), settling the effect as
//! failed. In a fleet of one that is fail-closed and fine. In a heterogeneous
//! fleet it means whichever worker wins the race decides, and a general
//! worker winning permanently kills work a sandboxed worker was standing by
//! to run.
//!
//! Routing happens at the *claim*, not at the refusal: a worker never takes a
//! lease on a ticket whose class it does not serve, so the ticket stays
//! claimable. The authority's `execution-policy-unroutable` refusal remains,
//! and is still load-bearing — it is the backstop for a ticket retagged
//! between claim and grant.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, RecordingToolExecutor};
use rakka_agent::{
    AgentEffectSpec, AgentExecutionPolicyRef, AgentModelTurn, AgentRunEffectStatus,
    AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    AgentToolRegistry, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;

use common::*;

const TOOL: &str = "charge-card";
const SANDBOXED: &str = "sandboxed-egress";
const GENERAL: &str = "general-purpose";
/// The class a strict-mode deployment runs the substrate's own effects under
/// — the model call above all, which carries no application declaration to
/// take a class from.
const SUBSTRATE: &str = "rakka-substrate";

fn tool_id() -> AgentToolId {
    AgentToolId::new(TOOL).expect("the tool id is valid")
}

fn policy(name: &str) -> AgentExecutionPolicyRef {
    AgentExecutionPolicyRef::new(name).expect("the policy ref is valid")
}

fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me do that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("the call id is valid"),
                tool_id(),
                serde_json::json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                .expect("the proposal is inline-bounded"),
        )
}

fn adapter() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn())
        .with_turn_for(2, proposing_turn())
}

/// A registry whose one tool is declared under an execution policy — or
/// under none, when `class` is absent.
fn registry_for(class: Option<&str>) -> AgentToolRegistry {
    let mut spec = AgentEffectSpec::idempotent(2).expect("the spec is valid");
    if let Some(class) = class {
        spec = spec.with_execution_policy(policy(class));
    }
    tool_registry_for_spec(TOOL, &spec)
}

/// A world whose one tool is routed to `class`, with a router that accepts
/// every class (the authority gate is not what is under test here).
fn routed_world(class: Option<&str>) -> AuthorityFixture {
    let registry = registry_for(class);
    let envelope = envelope_for_registry(&registry);
    AuthorityFixture::new(
        adapter(),
        AgentToolAuthority::new(registry).with_execution_router(Arc::new(AcceptAny)),
        None,
    )
    .with_envelope(envelope)
}

/// A router that accepts every class, so the *claim filter* is what decides.
struct AcceptAny;
impl rakka_agent::AgentExecutionPolicyRouter for AcceptAny {
    fn accepts(&self, _policy: &AgentExecutionPolicyRef) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// 1. A worker that does not serve the class never claims the ticket.
// ---------------------------------------------------------------------------

/// The ticket is left alone: no lease, no attempt, no durable write.
#[tokio::test]
async fn a_worker_that_does_not_serve_the_class_never_claims_its_ticket() {
    let fx = routed_world(Some(SANDBOXED));
    fx.start().await;
    fx.pump_until_tool_ticket().await;

    let general_tools = RecordingToolExecutor::new();
    let mut general = fx.worker("worker-general", general_tools.clone(), Some(&[GENERAL]));
    let pass = general
        .pump_run(&run_scope())
        .await
        .expect("the pass completes");

    assert_eq!(
        pass.claimed, 0,
        "the general worker took a lease it cannot use"
    );
    assert_eq!(
        pass.class_filtered, 1,
        "the skip must be counted, or a stalled fleet is indistinguishable from an idle one"
    );
    assert_eq!(pass.invoked, 0);
    assert_eq!(pass.failed_attempts, 0, "nothing durable was spent");
    assert_eq!(general_tools.invocation_count(TOOL), 0);
}

// ---------------------------------------------------------------------------
// 2. The headline: two workers, one ticket, the right one runs it.
// ---------------------------------------------------------------------------

/// The general worker passes first and claims nothing; the sandboxed worker
/// then claims and invokes exactly once.
///
/// Before claim-time filtering the first pass would have *failed the effect*,
/// and the second worker would have found nothing left to run.
#[tokio::test]
async fn two_workers_with_different_accept_sets_route_one_ticket_to_the_one_that_serves_it() {
    let fx = routed_world(Some(SANDBOXED));
    fx.start().await;
    fx.pump_until_tool_ticket().await;

    let general_tools = RecordingToolExecutor::new();
    let sandboxed_tools = RecordingToolExecutor::new();

    // The wrong worker goes first, deliberately: it is the one that used to
    // win the race and kill the effect.
    let mut general = fx.worker("worker-general", general_tools.clone(), Some(&[GENERAL]));
    let first = general
        .pump_run(&run_scope())
        .await
        .expect("the pass completes");
    assert_eq!(first.claimed, 0);
    assert_eq!(first.class_filtered, 1);

    let mut sandboxed = fx.worker(
        "worker-sandboxed",
        sandboxed_tools.clone(),
        Some(&[SANDBOXED]),
    );
    let second = sandboxed
        .pump_run(&run_scope())
        .await
        .expect("the pass completes");

    assert_eq!(
        second.claimed, 1,
        "the serving worker did not get the ticket"
    );
    assert_eq!(second.class_filtered, 0);
    assert_eq!(second.invoked, 1);
    assert_eq!(
        general_tools.invocation_count(TOOL),
        0,
        "the general worker ran a sandboxed effect"
    );
    assert_eq!(
        sandboxed_tools.invocation_count(TOOL),
        1,
        "the sandboxed worker did not run it exactly once"
    );
}

// ---------------------------------------------------------------------------
// 3. A class nobody serves is a visible backlog, not a silent stall.
// ---------------------------------------------------------------------------

/// Repeated passes keep reporting due work and keep filtering it, and the
/// ticket's attempt count never moves.
///
/// This is the cost of routing at the claim: an unroutable class waits
/// forever rather than failing fast. That is the right trade — a permanently
/// failed effect because the wrong worker won a race is worse — but only if
/// an operator can see it, which is what `class_filtered` beside
/// `due_dispatch_count` is for.
#[tokio::test]
async fn a_class_no_worker_serves_is_a_visible_filtered_backlog_not_a_silent_stall() {
    let fx = routed_world(Some("nobody-serves-this"));
    fx.start().await;
    fx.pump_until_tool_ticket().await;

    let tools = RecordingToolExecutor::new();
    for round in 0..3 {
        let mut worker = fx.worker("worker-general", tools.clone(), Some(&[GENERAL, SANDBOXED]));
        let pass = worker
            .pump_run(&run_scope())
            .await
            .expect("the pass completes");
        assert_eq!(
            pass.claimed, 0,
            "round {round} claimed an unservable ticket"
        );
        assert_eq!(
            pass.class_filtered, 1,
            "round {round} did not report the filtered ticket"
        );
        assert_eq!(pass.failed_attempts, 0, "round {round} spent an attempt");
    }
    assert_eq!(
        tools.invocation_count(TOOL),
        0,
        "an unservable class must never be invoked by anyone"
    );

    // And the effect is still waiting, not failed — a worker that serves the
    // class could still be deployed.
    assert_eq!(
        fx.effect_status(1).await,
        Some(AgentRunEffectStatus::Ready),
        "the ticket must stay live for a worker that can serve it"
    );
}

// ---------------------------------------------------------------------------
// 4. Unclassified intents: permissive by default, refusable under strict mode.
// ---------------------------------------------------------------------------

/// An intent naming no class is claimable by every worker.
#[tokio::test]
async fn an_unclassified_intent_is_claimable_by_every_worker() {
    let fx = routed_world(None);
    fx.start().await;
    fx.pump_until_tool_ticket().await;

    let tools = RecordingToolExecutor::new();
    let mut worker = fx.worker("worker-sandboxed", tools.clone(), Some(&[SANDBOXED]));
    let pass = worker
        .pump_run(&run_scope())
        .await
        .expect("the pass completes");

    assert_eq!(
        pass.claimed, 1,
        "unclassified work must not be stranded by a worker's class declaration"
    );
    assert_eq!(pass.class_filtered, 0);
    assert_eq!(tools.invocation_count(TOOL), 1);
}

/// Under strict mode the same intent is refused before dispatch.
///
/// The model call must have *run* for this to mean anything. Strict mode
/// applies to every intent, and the substrate's own effects carry no
/// application declaration to take a class from — so a switch that classified
/// only tools would refuse each run's turn-1 model call with this very same
/// code and terminate every run before the model was ever called. That
/// failure is indistinguishable from this one by the terminal code and by the
/// tool's invocation count alone, which is what the adapter assertion below
/// exists to separate.
#[tokio::test]
async fn an_unclassified_intent_is_refused_under_strict_mode() {
    let registry = registry_for(None);
    let envelope = envelope_for_registry(&registry);
    let fx = AuthorityFixture::new(
        adapter(),
        AgentToolAuthority::new(registry)
            .with_execution_router(Arc::new(AcceptAny))
            .with_required_execution_policy(policy(SUBSTRATE)),
        None,
    )
    .with_envelope(envelope);
    fx.start().await;
    fx.pump().await;

    assert!(
        fx.adapter.calls() >= 1,
        "strict mode refused the run's own model call: the substrate class \
         never reached the model spec, so no run can ever reach its first tool"
    );
    assert_eq!(
        fx.terminal_failure_code().await,
        "execution-policy-required",
        "a deployment that claims isolation must not run an unclassified effect"
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

/// The positive control: strict mode with every class named runs to
/// completion.
///
/// Without this the refusal test above passes just as well when the switch is
/// unsatisfiable by construction — the state the slice shipped in, where
/// enabling it terminated every run at turn 1.
#[tokio::test]
async fn strict_mode_completes_a_run_whose_every_intent_names_a_class() {
    let registry = registry_for(Some(SANDBOXED));
    let envelope = envelope_for_registry(&registry);
    let fx = AuthorityFixture::new(
        adapter(),
        AgentToolAuthority::new(registry)
            .with_execution_router(Arc::new(AcceptAny))
            .with_required_execution_policy(policy(SUBSTRATE)),
        None,
    )
    .with_envelope(envelope);
    fx.start().await;
    fx.pump().await;

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        rakka_agent::AgentRunStatus::Completed,
        "strict mode stopped a run every one of whose intents named a class: {:?}",
        run.terminal_reason
    );
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "the classified tool ran exactly once"
    );
}

/// The substrate class the authority requires is the one its own projection
/// stamps, so the gate and the specs that satisfy it cannot drift.
#[test]
fn the_required_substrate_class_is_the_one_the_projection_stamps() {
    use rakka_agent::{AgentContextSnapshotRef, AgentRunEffectRequest};

    let authority = AgentToolAuthority::new(registry_for(Some(SANDBOXED)))
        .with_execution_router(Arc::new(AcceptAny))
        .with_required_execution_policy(policy(SUBSTRATE));
    let policies = authority
        .effect_policies()
        .expect("the authority projects valid policies");

    let model = policies.spec_for(&AgentRunEffectRequest::Model {
        context: AgentContextSnapshotRef::for_turn(&run_scope(), 1)
            .expect("the snapshot reference derives"),
        profile: None,
    });
    assert_eq!(
        model.execution_policy.as_ref(),
        Some(&policy(SUBSTRATE)),
        "the model spec carries no class, so strict mode would refuse turn 1"
    );

    // A registered tool keeps the class its binding declared: the substrate
    // stamp must never overwrite what the deployment classified, or the
    // dispatch gate's binding-agreement check would refuse every tool.
    let tool = policies.spec_for(&AgentRunEffectRequest::Tool {
        call: Box::new(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("the call id is valid"),
                tool_id(),
                serde_json::json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        ),
    });
    assert_eq!(tool.execution_policy.as_ref(), Some(&policy(SANDBOXED)));
}

// ---------------------------------------------------------------------------
// 5. The two layers are independent, and the gate is still the backstop.
// ---------------------------------------------------------------------------

/// A worker whose filter admits a ticket its *authority* refuses still fails
/// closed at the grant.
///
/// The filter is placement; the authority is authorization. Neither is the
/// other's substitute, and this is what proves the second still runs when the
/// first lets something through — the retagged-ticket case in production.
#[tokio::test]
async fn a_worker_admitted_by_its_filter_still_fails_closed_at_the_grant() {
    struct AcceptOnly(&'static str);
    impl rakka_agent::AgentExecutionPolicyRouter for AcceptOnly {
        fn accepts(&self, policy: &AgentExecutionPolicyRef) -> bool {
            policy.as_str() == self.0
        }
    }

    let registry = registry_for(Some(SANDBOXED));
    let envelope = envelope_for_registry(&registry);
    let fx = AuthorityFixture::new(
        adapter(),
        // The authority accepts only the general class; the ticket is
        // sandboxed. The filter below admits it anyway.
        AgentToolAuthority::new(registry).with_execution_router(Arc::new(AcceptOnly(GENERAL))),
        None,
    )
    .with_envelope(envelope);
    fx.start().await;
    fx.pump_until_tool_ticket().await;

    let tools = RecordingToolExecutor::new();
    let mut worker = fx.worker("worker-confused", tools.clone(), Some(&[SANDBOXED]));
    let pass = worker
        .pump_run(&run_scope())
        .await
        .expect("the pass completes");

    assert_eq!(
        pass.claimed, 1,
        "the filter admitted the ticket, as configured"
    );
    assert_eq!(
        pass.invoked, 0,
        "nothing may be invoked when the grant is refused"
    );
    assert_eq!(
        tools.invocation_count(TOOL),
        0,
        "the authority gate did not stop an effect its filter let through"
    );
    assert_eq!(
        fx.terminal_failure_code().await,
        "execution-policy-unroutable"
    );
}
