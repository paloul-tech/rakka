//! The spans a run's child wait closes (issue #70,
//! [specification 17.4, 17.5 and 17.11](../../docs/plans/rakka-agent/spec.md)).
//!
//! A run parked on its closed, unresolved fan-in group closes a `wait`
//! segment on the commit that parks it, and the transition that resolves the
//! group closes `wait-resolve`, linking the park and whatever ended the wait.
//! The two commands that can end a child wait from outside the run carry an
//! optional trace context for that link.

mod common;

use common::*;
use rakka_agent::{AgentOperationId, AgentOperationKind, AgentRunEntityCommand};
use rakka_agent_workflow::AgentTelemetryContext;

/// A `FireFanInDeadline` as the build before issue #70 encoded it.
const DEADLINE_BEFORE_ISSUE_70: &str = r#"{"fire-fan-in-deadline":{"operation_id":"checkpoint-resolution/acme/support-agent/fan-in"}}"#;

/// A `RecordWorkflowResult` as the build before issue #70 encoded it.
const RESULT_BEFORE_ISSUE_70: &str = r#"{"record-workflow-result":{"operation_id":"workflow-result/acme/workflow-invocation-411d969117575115b5bcc76555cd824c97d8171ed098721d91d216daaee154ca/result","invocation":"workflow-invocation-411d969117575115b5bcc76555cd824c97d8171ed098721d91d216daaee154ca","child_run":"child-run-1","status":"completed","terminal_reason":null,"result_ref":null,"result_digest":null}}"#;

const HOST_PARENT: &str = "00-2cf7651916cd43dd8448eb211c80319e-d7ad6b7169203333-01";

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
