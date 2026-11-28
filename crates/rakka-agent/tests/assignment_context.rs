//! A run's trace context is the context of the assignment that created it
//! (issue #224, [specification 17.5](../../docs/plans/rakka-agent/spec.md)).
//!
//! The run used to record the context of every assignment exchange it was
//! handed, whatever it answered: a refused generation's context, or a repeat
//! of its own assignment under another context, became the run's, and every
//! segment and derived span identity afterwards followed it. Neither path is
//! reachable through upstream's own task entity, so these proofs build the
//! assignments themselves.

mod common;

use common::*;
use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{AgentAssignmentGeneration, AgentExchangeReply, AgentExchangeStatus};
use rakka_agent_workflow::AgentTelemetryContext;

const INGRESS_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const OTHER_PARENT: &str = "00-4ef7651916cd43dd8448eb211c8031a0-f7ad6b7169203335-01";

fn context(trace_parent: &str) -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(trace_parent.to_string()),
        ..AgentTelemetryContext::default()
    }
}

/// Delivers one exchange through a facade of its own and returns its reply.
async fn deliver_reply(
    fixture: &Fixture,
    envelope: &rakka_agent::AgentExchangeEnvelope,
) -> AgentExchangeReply {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.accept(envelope, &fixture.router, fixture.now())
        .await
        .expect("the delivery is answered")
}

fn refused_with(reply: &AgentExchangeReply) -> Option<&str> {
    match reply.result().status() {
        AgentExchangeStatus::Rejected { code, .. } => Some(code.as_str()),
        _ => None,
    }
}

/// A run whose assignment carried the ingress context.
async fn traced_run() -> Fixture {
    let fixture = Fixture::new(ScriptedDispatcher::new());
    fixture.instantiate_agent().await;
    fixture.create_task_traced(context(INGRESS_PARENT)).await;
    assert_eq!(
        run_context(&fixture).await.trace_parent.as_deref(),
        Some(INGRESS_PARENT),
        "the creating assignment's context is the run's"
    );
    fixture
}

/// An assignment for another generation is refused, and the refused
/// message's context does not become the run's.
#[tokio::test]
async fn a_refused_generation_leaves_the_runs_trace_context() {
    let fixture = traced_run().await;
    let before = run_context(&fixture).await;

    let reply = deliver_reply(
        &fixture,
        &assignment_envelope(&fixture, AgentAssignmentGeneration::new(2))
            .with_telemetry(context(OTHER_PARENT)),
    )
    .await;

    assert_eq!(refused_with(&reply), Some("run-generation-conflict"));
    assert_eq!(run_context(&fixture).await, before);
}

/// The run's own assignment, re-sent past the journal's window under another
/// context, is answered with the original acceptance by a transition that
/// runs again, and leaves the run's context as it was.
#[tokio::test]
async fn the_same_generation_sent_again_leaves_the_runs_trace_context() {
    let fixture = traced_run().await;
    let before = run_context(&fixture).await;
    forget_exchanges(&fixture).await;

    let reply = deliver_reply(
        &fixture,
        &assignment_envelope(&fixture, AgentAssignmentGeneration::new(1))
            .with_telemetry(context(OTHER_PARENT)),
    )
    .await;

    assert!(reply.result().is_accepted(), "{:?}", reply.result());
    assert!(
        !reply.is_replayed(),
        "past the window the transition runs again, not the journal"
    );
    assert_eq!(run_context(&fixture).await, before);
}

/// A run created without a trace context takes none from a later
/// assignment, whether a repeat of its own or another generation's: an
/// entity never invents a context, and it adopts none after the fact.
#[tokio::test]
async fn a_run_created_without_a_context_takes_none_from_a_later_assignment() {
    let fixture = Fixture::new(ScriptedDispatcher::new());
    fixture.instantiate_agent().await;
    fixture.create_task().await;
    assert_eq!(
        run_context(&fixture).await,
        AgentTelemetryContext::default()
    );

    forget_exchanges(&fixture).await;
    let repeat = deliver_reply(
        &fixture,
        &assignment_envelope(&fixture, AgentAssignmentGeneration::new(1))
            .with_telemetry(context(OTHER_PARENT)),
    )
    .await;
    assert!(repeat.result().is_accepted(), "{:?}", repeat.result());
    assert!(!repeat.is_replayed());
    assert_eq!(
        run_context(&fixture).await,
        AgentTelemetryContext::default()
    );

    let refused = deliver_reply(
        &fixture,
        &assignment_envelope(&fixture, AgentAssignmentGeneration::new(2))
            .with_telemetry(context(OTHER_PARENT)),
    )
    .await;
    assert_eq!(refused_with(&refused), Some("run-generation-conflict"));
    assert_eq!(
        run_context(&fixture).await,
        AgentTelemetryContext::default()
    );
}
