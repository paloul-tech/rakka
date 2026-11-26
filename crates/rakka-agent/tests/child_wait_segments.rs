//! The spans a run's child wait closes (issue #70,
//! [specification 17.4, 17.5 and 17.11](../../docs/plans/rakka-agent/spec.md)).
//!
//! A run parked on its closed, unresolved fan-in group closes a `wait`
//! segment on the commit that parks it, and the transition that resolves the
//! group closes `wait-resolve`, linking the park and whatever ended the wait.
//! The two commands that can end a child wait from outside the run carry an
//! optional trace context for that link.

mod common;

use std::sync::Arc;

use common::*;
use rakka_agent::{
    AgentFanInCell, AgentLoopPhase, AgentOperationId, AgentOperationKind, AgentRunEntityCommand,
    AgentTaskStatus, AgentTelemetrySegment, InMemoryAgentSegmentSink, AGENT_WAIT_KIND_CHILDREN,
};
use rakka_agent_workflow::AgentTelemetryContext;

/// A `FireFanInDeadline` as the build before issue #70 encoded it.
const DEADLINE_BEFORE_ISSUE_70: &str = r#"{"fire-fan-in-deadline":{"operation_id":"checkpoint-resolution/acme/support-agent/fan-in"}}"#;

/// A `RecordWorkflowResult` as the build before issue #70 encoded it.
const RESULT_BEFORE_ISSUE_70: &str = r#"{"record-workflow-result":{"operation_id":"workflow-result/acme/workflow-invocation-411d969117575115b5bcc76555cd824c97d8171ed098721d91d216daaee154ca/result","invocation":"workflow-invocation-411d969117575115b5bcc76555cd824c97d8171ed098721d91d216daaee154ca","child_run":"child-run-1","status":"completed","terminal_reason":null,"result_ref":null,"result_digest":null}}"#;

const HOST_PARENT: &str = "00-2cf7651916cd43dd8448eb211c80319e-d7ad6b7169203333-01";
const INGRESS_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

fn context(trace_parent: &str) -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(trace_parent.to_string()),
        ..AgentTelemetryContext::default()
    }
}

/// The context a command carries for the resolution's `resume-request` link.
fn carried(command: &AgentRunEntityCommand) -> &AgentTelemetryContext {
    match command {
        AgentRunEntityCommand::FireFanInDeadline { telemetry, .. }
        | AgentRunEntityCommand::RecordWorkflowResult { telemetry, .. } => telemetry,
        other => panic!("not a child-wait command: {other:?}"),
    }
}

/// A command a build before issue #70 wrote has no `telemetry` key, and
/// decodes to an empty context; one built with a context encodes the key and
/// round-trips. An older build ignores the key: the command enum denies no
/// unknown field.
#[test]
fn commands_without_a_trace_context_decode_to_an_empty_one() {
    for literal in [DEADLINE_BEFORE_ISSUE_70, RESULT_BEFORE_ISSUE_70] {
        let command: AgentRunEntityCommand =
            serde_json::from_str(literal).expect("a command written before issue #70 decodes");
        assert_eq!(carried(&command), &AgentTelemetryContext::default());

        let traced = command.clone().with_telemetry(context(HOST_PARENT));
        assert_eq!(
            carried(&traced).trace_parent.as_deref(),
            Some(HOST_PARENT),
            "the host's context is carried"
        );
        let encoded = serde_json::to_value(&traced).expect("the command encodes");
        let body = encoded
            .as_object()
            .and_then(|tagged| tagged.values().next())
            .expect("an externally tagged variant");
        assert!(
            body.get("telemetry").is_some(),
            "the key is written: {encoded}"
        );
        let decoded: AgentRunEntityCommand =
            serde_json::from_value(encoded).expect("the command decodes");
        assert_eq!(decoded, traced);
    }
}

/// Every other command is returned unchanged.
#[test]
fn a_trace_context_is_set_only_on_the_two_child_wait_commands() {
    let timers = AgentRunEntityCommand::FireCheckpointTimers {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            "timers",
        )
        .expect("the operation id derives"),
    };
    assert_eq!(timers.clone().with_telemetry(context(HOST_PARENT)), timers);
}

/// The segments of one class the sink holds, in the order they closed.
fn closed(sink: &InMemoryAgentSegmentSink, label: &str) -> Vec<AgentTelemetrySegment> {
    sink.segments()
        .into_iter()
        .filter(|segment| segment.operation.as_label() == label)
        .collect()
}

/// The run's fan-in group and its trace context, read from the durable
/// record.
async fn group_and_context(fixture: &Fixture) -> (AgentFanInCell, AgentTelemetryContext) {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let state = run.state().expect("state");
    let loop_state = state.loop_state().expect("the loop is running");
    (
        loop_state.fan_in().expect("the group exists").clone(),
        loop_state.telemetry().clone(),
    )
}

async fn phase_of(fixture: &Fixture) -> AgentLoopPhase {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.state()
        .expect("state")
        .loop_state()
        .expect("the loop is running")
        .phase()
}

/// A traced fan-out run, pumped until it rests on its two children.
async fn parked_fan_out(sink: &Arc<InMemoryAgentSegmentSink>) -> Fixture {
    let fixture = fan_out_fixture(SkillNamedExecutor::new()).with_segments(sink.clone());
    create_fan_out_task_traced(&fixture, None, context(INGRESS_PARENT)).await;
    fixture.pump().await.expect("the loop should converge");
    assert_eq!(phase_of(&fixture).await, AgentLoopPhase::AwaitingChildren);
    fixture
}

/// The commit that parks a run on its two children closes one `wait`
/// segment, exported under the identity derived from the run's context and
/// the group, as the durable record holds them.
#[tokio::test]
async fn a_turn_awaiting_two_children_parks_one_wait_segment() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fixture = parked_fan_out(&sink).await;

    let waits = closed(&sink, "wait");
    assert_eq!(waits.len(), 1, "one park: {:?}", sink.operations());
    let wait = &waits[0];
    assert_eq!(
        wait.operation,
        rakka_agent::AgentSegmentOperation::Wait {
            kind: AGENT_WAIT_KIND_CHILDREN
        }
    );
    assert_eq!(
        wait.attributes.get(rakka_agent::SEGMENT_ATTR_WAIT_KIND),
        Some(&AGENT_WAIT_KIND_CHILDREN.to_string())
    );
    assert_eq!(
        wait.attributes.get(rakka_agent::SEGMENT_ATTR_FAN_IN_POLICY),
        Some(&"all".to_string())
    );
    let (group, run_context) = group_and_context(&fixture).await;
    let identity = group
        .parked_span_identity(&run_context)
        .expect("a traced run derives the park's identity");
    assert_eq!(wait.span_id.as_deref(), Some(identity.span_id.as_str()));
    assert!(closed(&sink, "wait-resolve").is_empty());
}

/// A child's result that leaves the group open parks nothing more, and
/// resolves nothing.
#[tokio::test]
async fn a_result_that_leaves_the_group_open_parks_nothing_more() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fixture = parked_fan_out(&sink).await;
    let children = committed_children(&fixture).await;
    let first = child_result_envelope(
        &fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    let reply = run
        .accept(&first, &fixture.router, fixture.now())
        .await
        .expect("the delivery succeeds");
    assert!(reply.result().is_accepted());
    assert_eq!(phase_of(&fixture).await, AgentLoopPhase::AwaitingChildren);

    assert_eq!(closed(&sink, "wait").len(), 1, "{:?}", sink.operations());
    assert!(closed(&sink, "wait-resolve").is_empty());
}

/// A run with no trace context derives no identity: the park still closes,
/// under a minted span id.
#[tokio::test]
async fn a_run_with_no_trace_context_still_parks() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fixture = fan_out_fixture(SkillNamedExecutor::new()).with_segments(sink.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");
    assert_eq!(phase_of(&fixture).await, AgentLoopPhase::AwaitingChildren);

    let waits = closed(&sink, "wait");
    assert_eq!(waits.len(), 1, "{:?}", sink.operations());
    assert_eq!(waits[0].span_id, None);
}
