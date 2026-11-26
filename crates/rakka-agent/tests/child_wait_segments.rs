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
use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentExchangeEnvelope, AgentFanInCell, AgentLoopPhase, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentRunEntityCommand, AgentTaskStatus, AgentTelemetrySegment,
    AgentToolCallId, AgentToolCallRequest, InMemoryAgentSegmentSink, AGENT_WAIT_KIND_CHILDREN,
    ATTR_AGENT_TELEMETRY_LINK_KIND, CURRENT_AGENT_LOOP_ADAPTER_VERSION, LINK_KIND_PARKED_WAIT,
    LINK_KIND_RESUME_REQUEST,
};
use rakka_agent_workflow::{AgentSpanLink, AgentTelemetryContext};

/// A `FireFanInDeadline` as the build before issue #70 encoded it.
const DEADLINE_BEFORE_ISSUE_70: &str = r#"{"fire-fan-in-deadline":{"operation_id":"checkpoint-resolution/acme/support-agent/fan-in"}}"#;

/// A `RecordWorkflowResult` as the build before issue #70 encoded it.
const RESULT_BEFORE_ISSUE_70: &str = r#"{"record-workflow-result":{"operation_id":"workflow-result/acme/workflow-invocation-411d969117575115b5bcc76555cd824c97d8171ed098721d91d216daaee154ca/result","invocation":"workflow-invocation-411d969117575115b5bcc76555cd824c97d8171ed098721d91d216daaee154ca","child_run":"child-run-1","status":"completed","terminal_reason":null,"result_ref":null,"result_digest":null}}"#;

const HOST_PARENT: &str = "00-2cf7651916cd43dd8448eb211c80319e-d7ad6b7169203333-01";
const INGRESS_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const CHILD_PARENT: &str = "00-3df7651916cd43dd8448eb211c80319f-e7ad6b7169203334-01";

/// The span id a W3C `traceparent` names.
fn span_of(trace_parent: &str) -> &str {
    trace_parent.split('-').nth(2).expect("a span id")
}

/// The link of `kind` a segment carries, when it carries one.
fn link_of<'a>(segment: &'a AgentTelemetrySegment, kind: &str) -> Option<&'a AgentSpanLink> {
    segment
        .telemetry
        .span_links
        .iter()
        .find(|link| link.attributes.get(ATTR_AGENT_TELEMETRY_LINK_KIND) == Some(&kind.to_string()))
}

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
            serde_json::from_value(encoded.clone()).expect("the command decodes");
        assert_eq!(decoded, traced);

        // What an older build does with `telemetry`: a key its variant does
        // not declare is ignored, here another one this build does not know.
        let mut newer = encoded;
        newer
            .as_object_mut()
            .and_then(|tagged| tagged.values_mut().next())
            .and_then(|body| body.as_object_mut())
            .expect("an externally tagged struct variant")
            .insert("from_a_newer_build".to_string(), serde_json::json!(1));
        let tolerated: AgentRunEntityCommand =
            serde_json::from_value(newer).expect("an unknown key is ignored");
        assert_eq!(tolerated, traced);
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

/// Delivers one exchange to the run through a facade of its own, as a
/// re-activated, passivated owner takes it.
async fn deliver(
    fixture: &Fixture,
    envelope: &AgentExchangeEnvelope,
) -> Result<bool, rakka_agent::AgentRunError> {
    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    run.accept(envelope, &fixture.router, fixture.now())
        .await
        .map(|reply| reply.result().is_accepted())
}

/// The two children's results, the second under the child's trace context.
fn results(
    fixture: &Fixture,
    children: &[(rakka_agent::AgentDelegationId, rakka_agent::AgentTaskId)],
) -> (AgentExchangeEnvelope, AgentExchangeEnvelope) {
    let first = child_result_envelope(
        fixture,
        &children[0].0,
        &children[0].1,
        AgentTaskStatus::Completed,
    );
    let second = child_result_envelope(
        fixture,
        &children[1].0,
        &children[1].1,
        AgentTaskStatus::Completed,
    )
    .with_telemetry(context(CHILD_PARENT));
    (first, second)
}

/// The last child's result resolves the group the run was parked on, and
/// that transition closes one `wait-resolve` segment, linking the park
/// (`parked-wait`, the `wait` segment's own span id) and the child's span
/// (`resume-request`). Each delivery is taken by a facade of its own, so the
/// park and the resolution derive the identity independently: the
/// passivated case. No `run-resume` closes, as before: the pass goes on to
/// the next model call, which is the run's next wait.
#[tokio::test]
async fn the_last_childs_result_resolves_the_wait_and_links_the_park_and_the_child() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fixture = parked_fan_out(&sink).await;
    let resumes_before = closed(&sink, "run-resume").len();
    let children = committed_children(&fixture).await;
    let (first, second) = results(&fixture, &children);
    assert!(deliver(&fixture, &first)
        .await
        .expect("the first result is taken"));
    assert!(deliver(&fixture, &second)
        .await
        .expect("the second result is taken"));

    let resolves = closed(&sink, "wait-resolve");
    assert_eq!(resolves.len(), 1, "{:?}", sink.operations());
    let resolve = &resolves[0];
    let wait = &closed(&sink, "wait")[0];
    assert!(wait.span_id.is_some(), "a traced park has an identity");
    assert_eq!(
        link_of(resolve, LINK_KIND_PARKED_WAIT).map(|link| link.span_id.as_str()),
        wait.span_id.as_deref(),
        "the resolution links the park it ended"
    );
    assert_eq!(
        link_of(resolve, LINK_KIND_RESUME_REQUEST).map(|link| link.span_id.as_str()),
        Some(span_of(CHILD_PARENT)),
        "and the child's span that caused it"
    );
    let (group, _) = group_and_context(&fixture).await;
    assert_eq!(
        resolve
            .attributes
            .get(rakka_agent::SEGMENT_ATTR_FAN_IN_RESOLUTION),
        group.resolution.as_ref().map(|resolution| &resolution.code),
    );
    assert_eq!(
        resolve.attributes.get(rakka_agent::SEGMENT_ATTR_WAIT_KIND),
        Some(&AGENT_WAIT_KIND_CHILDREN.to_string())
    );
    assert_eq!(
        closed(&sink, "run-resume").len(),
        resumes_before,
        "no run-resume, as before"
    );
}

/// A duplicate of the result that resolved the group resolves nothing more,
/// in the journal's window or past it, where a conflicting duplicate re-runs
/// the transition: the run is no longer parked on the group.
#[tokio::test]
async fn a_duplicate_result_resolves_nothing() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fixture = parked_fan_out(&sink).await;
    let children = committed_children(&fixture).await;
    let (first, second) = results(&fixture, &children);
    deliver(&fixture, &first)
        .await
        .expect("the first result is taken");
    deliver(&fixture, &second)
        .await
        .expect("the second result is taken");
    deliver(&fixture, &second)
        .await
        .expect("the duplicate is answered");
    // A conflicting duplicate under the same operation id re-runs the
    // transition, and the first writer wins.
    let conflicting = child_result_envelope(
        &fixture,
        &children[1].0,
        &children[1].1,
        AgentTaskStatus::Cancelled,
    );
    let _ = deliver(&fixture, &conflicting).await;
    fixture.pump().await.expect("the loop should converge");
    assert_eq!(
        closed(&sink, "wait-resolve").len(),
        1,
        "{:?}",
        sink.operations()
    );
    assert_eq!(closed(&sink, "wait").len(), 1);
}

/// A one-member fan-out awaited under a wait deadline.
fn deadline_fixture(sink: &Arc<InMemoryAgentSegmentSink>) -> Fixture {
    Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn(
                    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                        .with_text("Fanning out with a wait deadline.")
                        .with_tool_call(
                            AgentToolCallRequest::new(
                                AgentToolCallId::new("delegate-1").expect("call id"),
                                delegation_tool_id(),
                                serde_json::json!({ "skill": SKILL, "input": { "text": "hello" } }),
                            )
                            .expect("the tool call is bounded"),
                        )
                        .with_tool_call(
                            AgentToolCallRequest::new(
                                AgentToolCallId::new("await-1").expect("call id"),
                                fan_in_tool_id(),
                                serde_json::json!({ "deadline": 5_000 }),
                            )
                            .expect("the tool call is bounded"),
                        ),
                )
                .with_turn(proposing_turn()),
        )
        .with_a2a_send_executor(SkillNamedExecutor::new()),
    )
    .with_delegation(delegation_config_with_fan_in())
    .with_segments(sink.clone())
}

/// The deadline ends the wait: `wait-resolve` links the park, and the
/// scheduler's span only when the command carried it.
#[tokio::test]
async fn the_deadline_resolves_the_wait_and_links_its_timer_when_given_one() {
    for traced_timer in [false, true] {
        let sink = Arc::new(InMemoryAgentSegmentSink::new());
        let fixture = deadline_fixture(&sink);
        create_fan_out_task_traced(&fixture, None, context(INGRESS_PARENT)).await;
        fixture.pump().await.expect("the loop should converge");
        assert_eq!(phase_of(&fixture).await, AgentLoopPhase::AwaitingChildren);

        fixture
            .clock
            .store(10_000, std::sync::atomic::Ordering::SeqCst);
        let mut command = AgentRunEntityCommand::FireFanInDeadline {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Command,
                [TENANT, "fan-in-deadline", "due"],
            )
            .expect("the operation id derives"),
            telemetry: AgentTelemetryContext::default(),
        };
        if traced_timer {
            command = command.with_telemetry(context(HOST_PARENT));
        }
        let mut run = fixture.run();
        run.recover(fixture.now()).await.expect("recover");
        run.apply(command, &fixture.router, fixture.now())
            .await
            .expect("the due fire applies");

        let resolves = closed(&sink, "wait-resolve");
        assert_eq!(
            resolves.len(),
            1,
            "traced timer {traced_timer}: {:?}",
            sink.operations()
        );
        let wait = &closed(&sink, "wait")[0];
        assert_eq!(
            link_of(&resolves[0], LINK_KIND_PARKED_WAIT).map(|link| link.span_id.as_str()),
            wait.span_id.as_deref()
        );
        assert_eq!(
            link_of(&resolves[0], LINK_KIND_RESUME_REQUEST).map(|link| link.span_id.as_str()),
            traced_timer.then(|| span_of(HOST_PARENT)),
            "traced timer {traced_timer}"
        );
    }
}

/// A run with no trace context has no park identity to link: its
/// resolution still closes, with the trigger's link alone.
#[tokio::test]
async fn a_run_with_no_trace_context_resolves_without_a_park_link() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fixture = fan_out_fixture(SkillNamedExecutor::new()).with_segments(sink.clone());
    create_fan_out_task(&fixture, None).await;
    fixture.pump().await.expect("the loop should converge");
    let children = committed_children(&fixture).await;
    let (first, second) = results(&fixture, &children);
    deliver(&fixture, &first)
        .await
        .expect("the first result is taken");
    deliver(&fixture, &second)
        .await
        .expect("the second result is taken");

    let resolves = closed(&sink, "wait-resolve");
    assert_eq!(resolves.len(), 1, "{:?}", sink.operations());
    assert!(link_of(&resolves[0], LINK_KIND_PARKED_WAIT).is_none());
    assert_eq!(
        link_of(&resolves[0], LINK_KIND_RESUME_REQUEST).map(|link| link.span_id.as_str()),
        Some(span_of(CHILD_PARENT))
    );
}

/// The resolution is the transition, not the work after it: a result whose
/// settle pass loses its write answers with an error, and its `wait-resolve`
/// has closed, once.
#[tokio::test]
async fn a_resolution_whose_settle_pass_fails_still_closes_wait_resolve() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fixture = parked_fan_out(&sink).await;
    let children = committed_children(&fixture).await;
    let (first, second) = results(&fixture, &children);
    deliver(&fixture, &first)
        .await
        .expect("the first result is taken");

    let mut run = fixture.run();
    run.recover(fixture.now()).await.expect("recover");
    // Write 1 is the acceptance that resolves the group. Write 2 is the
    // crank's first compare-and-set, which a second writer beats.
    fixture.runs.crash_at(2, CrashPoint::ConflictBeforeWrite);
    let taken = run.accept(&second, &fixture.router, fixture.now()).await;
    fixture
        .runs
        .assert_crash_fired(2, CrashPoint::ConflictBeforeWrite);
    fixture.runs.survive();
    assert!(taken.is_err(), "the crank lost its write: {taken:?}");
    assert_eq!(
        closed(&sink, "wait-resolve").len(),
        1,
        "{:?}",
        sink.operations()
    );

    fixture.pump().await.expect("the loop should converge");
    assert_eq!(
        closed(&sink, "wait-resolve").len(),
        1,
        "the re-driven run is no longer parked: {:?}",
        sink.operations()
    );
}
