//! Bounded operation segments, closed on the path a run actually takes.
//!
//! Specification: [17.4](../../../docs/plans/rakka-agent/spec.md) (bounded
//! trace segments), [17.6](../../../docs/plans/rakka-agent/spec.md) (the
//! required span model), [17.20](../../../docs/plans/rakka-agent/spec.md)
//! (the agent domain keeps its own stable vocabulary and puts the
//! OpenTelemetry mapping behind the `otel` feature).
//!
//! This suite exists because the mapping it feeds had none. The `otel` module
//! shipped fully unit-tested and entirely unreachable — nothing in the
//! workspace constructed one of its operations outside its own test block —
//! and the corrective is a call site on the path a run actually takes, not
//! more unit tests. So these assertions are about *emission*: which
//! operations a real run closes, in what order, carrying which identity and
//! which trace context. The convention mapping is asserted separately, over
//! the segments this produces.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentEffectSpec, AgentModelTurn, AgentSegmentOperation, AgentSegmentOutcome, AgentTaskContent,
    AgentToolCallId, AgentToolCallRequest, AgentToolId, AgentToolRegistry,
    InMemoryAgentSegmentSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentTelemetryContext;

mod common;

use common::*;

const INGRESS_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

fn ingress_context() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(INGRESS_PARENT.to_string()),
        ..AgentTelemetryContext::default()
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
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
}

fn full_run_dispatcher() -> ScriptedDispatcher {
    ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    )
}

/// A run closes a segment for every loop transition it commits and one for
/// each resident execution slice, and each carries the run's identity and the
/// trace context the ingress command supplied.
#[tokio::test]
async fn a_run_closes_a_segment_for_every_committed_transition() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = Fixture::new(full_run_dispatcher()).with_segments(sink.clone());
    fx.instantiate_agent().await;
    fx.create_task_traced(ingress_context()).await;
    fx.pump().await.expect("the loop should run to completion");

    let segments = sink.segments();
    assert!(!segments.is_empty(), "a wired run closes segments");

    let decides = segments
        .iter()
        .filter(|segment| matches!(segment.operation, AgentSegmentOperation::Decide { .. }))
        .count();
    let invocations = segments
        .iter()
        .filter(|segment| matches!(segment.operation, AgentSegmentOperation::InvokeAgent { .. }))
        .count();
    assert!(decides > 0, "committed transitions close decide segments");
    // A resume is a durable wait being discharged — here, the model result
    // arriving at a run parked in `AwaitingModel`. It is closed only when the
    // wait actually ended, so a duplicate or a refusal closes none.
    assert!(
        segments
            .iter()
            .any(|segment| matches!(segment.operation, AgentSegmentOperation::RunResume)),
        "discharging a durable wait closes a resume segment: {:?}",
        sink.operations()
    );
    // The schedule row ends after durable acceptance into the outbox, not
    // after the transition that marked the effect ready — `Ready` does not
    // prove the sink write landed.
    assert!(
        segments.iter().any(|segment| matches!(
            segment.operation,
            AgentSegmentOperation::EffectSchedule { .. }
        )),
        "handing an effect to the outbox closes a schedule segment: {:?}",
        sink.operations()
    );
    assert!(
        invocations > 0,
        "each resident execution slice closes one invocation segment"
    );
    assert!(
        decides > invocations,
        "a resident slice advances more than one transition, so decides outnumber invocations"
    );

    // A segment is closed only after its transition committed, so every one
    // of them describes work that actually happened.
    assert!(segments
        .iter()
        .all(|segment| segment.outcome == AgentSegmentOutcome::Ok));

    // Identity rides the segment as an access-controlled attribute; the run
    // is the durable session identity of specification 17.3.
    let scope = run_scope();
    for segment in &segments {
        assert_eq!(
            segment.identity.run.as_deref(),
            Some(scope.run().as_str()),
            "every segment names the run it belongs to"
        );
        assert_eq!(
            segment.identity.agent.as_deref(),
            Some(scope.agent().as_str())
        );
    }

    // The persisted context is what links a segment to the operation that
    // caused it, across every passivation in between.
    assert!(
        segments
            .iter()
            .any(|segment| segment.telemetry.trace_parent.as_deref() == Some(INGRESS_PARENT)),
        "the ingress trace context reaches the segments the run closes"
    );

    // The invocation segment reports how much it advanced, as a bounded
    // number rather than as any identifier.
    let invocation = segments
        .iter()
        .find(|segment| matches!(segment.operation, AgentSegmentOperation::InvokeAgent { .. }))
        .expect("an invocation segment was closed");
    assert!(invocation
        .attributes
        .contains_key("rakka.agent.loop.transitions"));
}

/// Every segment names a bounded operation class and embeds no identifier in
/// it: the identity is an attribute, never part of the operation's name
/// (specification 17.6).
#[tokio::test]
async fn a_segments_operation_class_is_bounded_and_carries_no_identifier() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = Fixture::new(full_run_dispatcher()).with_segments(sink.clone());
    fx.instantiate_agent().await;
    fx.create_task_traced(ingress_context()).await;
    fx.pump().await.expect("the loop should run to completion");

    let scope = run_scope();
    for segment in sink.segments() {
        let label = segment.operation.as_label();
        assert!(
            !label.is_empty() && label.chars().all(|c| c.is_ascii_lowercase() || c == '-'),
            "{label} is not a stable kebab-case class"
        );
        // The agent name is deliberately absent: an `AgentId` is an
        // identifier however bounded it is, and 17.6 forbids one in a span
        // name. It rides the identity instead.
        if let AgentSegmentOperation::InvokeAgent { agent_name } = &segment.operation {
            assert!(
                agent_name.is_none(),
                "the run entity must not name the agent in the operation"
            );
        }
        let rendered = format!("{:?}", segment.operation);
        assert!(
            !rendered.contains(scope.run().as_str()),
            "an identifier reached the operation class: {rendered}"
        );
    }
}

/// An unwired run closes nothing and behaves identically: telemetry is never
/// a correctness input.
#[tokio::test]
async fn an_unwired_run_closes_no_segments() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = Fixture::new(full_run_dispatcher());
    fx.instantiate_agent().await;
    fx.create_task_traced(ingress_context()).await;
    fx.pump().await.expect("the loop should run to completion");

    assert!(
        sink.segments().is_empty(),
        "a sink the run was never wired with receives nothing"
    );
    assert_eq!(
        fx.run_snapshot().await.map(|snapshot| snapshot.status),
        Some(rakka_agent::AgentRunStatus::Completed),
        "the unwired run reaches the same terminal state"
    );
}

/// The dispatcher closes its own segments, and they come from the real
/// pipeline rather than from the scripted driver.
///
/// This distinction is the reason the test exists.
/// `ScriptedDispatcher::drive` bypasses `dispatch.rs` entirely — it awaits the
/// model adapter and applies `RecordEffectResult` directly — so the model,
/// tool, authorize, and dispatch rows are invisible to every `Fixture`-based
/// test however green it is. Instrumenting the scripted driver would have made
/// them visible and proved the testkit; driving the real `AgentRunEffectDispatcher`
/// proves the product.
#[tokio::test]
async fn the_real_dispatch_pipeline_closes_its_own_segments() {
    const TOOL: &str = "charge-card";

    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let registry = AgentToolRegistry::new()
        .register(tool_binding_for_spec(
            TOOL,
            &AgentEffectSpec::non_idempotent(),
        ))
        .expect("the tool registers");
    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn(TOOL))
        .with_turn_for(2, proposing_turn("charged"));
    let fx = AuthorityFixture::over(adapter, registry, None);
    fx.start().await;

    let mut pipeline = fx.pipeline().with_segments(sink.clone());
    for _round in 0..16 {
        fx.settle().await;
        let pass = pipeline
            .pump_run(&run_scope())
            .await
            .expect("the dispatch pass runs");
        let terminal = fx
            .fx
            .run_snapshot()
            .await
            .is_some_and(|run| run.status.is_terminal());
        if terminal {
            break;
        }
        if pass.registered == 0 && pass.claimed == 0 && pass.delivered == 0 {
            break;
        }
    }

    // The attributes specification 17.16's retention classes select on ride
    // the attempt that produced them, so a tail-sampling policy has something
    // to match rather than a rule that silently keeps nothing.
    let dispatch = sink
        .segments()
        .into_iter()
        .find(|segment| {
            matches!(
                segment.operation,
                AgentSegmentOperation::EffectDispatch { .. }
            )
        })
        .expect("a dispatch segment was closed");
    assert_eq!(
        dispatch
            .attributes
            .get(rakka_agent::SEGMENT_ATTR_EFFECT_STATUS)
            .map(String::as_str),
        Some("succeeded"),
        "the resolved effect status must be selectable"
    );
    assert!(dispatch
        .attributes
        .contains_key(rakka_agent::SEGMENT_ATTR_EFFECT_ATTEMPT));
    assert!(dispatch
        .attributes
        .contains_key(rakka_agent::SEGMENT_ATTR_SETTINGS_REVISION));

    let operations = sink.operations();
    for expected in ["tool-authorize", "effect-dispatch", "model-inference"] {
        assert!(
            operations.contains(&expected),
            "the dispatcher closed no `{expected}` segment; it closed {operations:?}"
        );
    }
    assert!(
        operations.contains(&"execute-tool"),
        "the tool row comes from the real executor, not the scripted driver: {operations:?}"
    );

    // Authorization ends before the durable `Started` write, so it is a
    // distinct interval from the attempt that follows it — and the attempt
    // never precedes its own grant.
    let authorize = operations
        .iter()
        .position(|operation| *operation == "tool-authorize")
        .expect("an authorize segment was closed");
    let dispatch = operations
        .iter()
        .position(|operation| *operation == "effect-dispatch")
        .expect("a dispatch segment was closed");
    assert!(authorize < dispatch);

    for segment in sink.segments() {
        assert_eq!(segment.outcome, AgentSegmentOutcome::Ok);
        assert!(segment.telemetry.trace_parent.is_some() || segment.identity.run.is_some());
    }
}

/// A checkpoint park closes its segment after the durable park, and names the
/// bounded checkpoint kind a retention policy selects escalation and timeout
/// on.
///
/// Specification 17.11 requires the opening span to end once the wait and its
/// notification are accepted, and forbids holding a span object across the
/// wait — so this asserts the segment exists *and* that the run is parked when
/// it does, which is the pair that makes the claim meaningful.
#[tokio::test]
async fn a_checkpoint_park_closes_its_segment_and_holds_none_open() {
    const TOOL: &str = "charge-card";

    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let registry = AgentToolRegistry::new()
        .register(
            tool_binding_for_spec(TOOL, &AgentEffectSpec::non_idempotent())
                .with_checkpoint_required(),
        )
        .expect("the tool registers");
    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn(TOOL))
        .with_turn_for(2, proposing_turn("charged"));
    // The checkpoint park is a *run entity* transition, so the sink has to
    // reach the entity as well as the dispatch pipeline. They are separate
    // wirings because in a deployment they are separate processes.
    let fx = AuthorityFixture::over(adapter, registry, None).with_segments(sink.clone());
    fx.start().await;

    let mut pipeline = fx.pipeline().with_segments(sink.clone());
    for _round in 0..16 {
        fx.settle().await;
        let pass = pipeline
            .pump_run(&run_scope())
            .await
            .expect("the dispatch pass runs");
        let parked = fx
            .fx
            .run_snapshot()
            .await
            .is_some_and(|run| run.status == rakka_agent::AgentRunStatus::WaitingForApproval);
        if parked {
            break;
        }
        if pass.registered == 0 && pass.claimed == 0 && pass.delivered == 0 {
            break;
        }
    }

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        rakka_agent::AgentRunStatus::WaitingForApproval,
        "the fixture must reach the checkpoint wait, or this proves nothing"
    );

    let checkpoints: Vec<_> = sink
        .segments()
        .into_iter()
        .filter(|segment| matches!(segment.operation, AgentSegmentOperation::CheckpointOpen))
        .collect();
    assert_eq!(
        checkpoints.len(),
        1,
        "the park closes exactly one checkpoint segment: {:?}",
        sink.operations()
    );
    assert!(checkpoints[0]
        .attributes
        .contains_key(rakka_agent::SEGMENT_ATTR_CHECKPOINT_KIND));
    // Ended, not held: the segment has both endpoints while the run is still
    // parked, which is what "no span object is held during passive wait"
    // means in a system that never holds one at all.
    assert!(checkpoints[0].duration_ms().is_some());
    assert_eq!(checkpoints[0].outcome, AgentSegmentOutcome::Ok);
}
