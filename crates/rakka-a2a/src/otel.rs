//! The A2A edge's OpenTelemetry mapping (`otel` feature).
//!
//! The agent domain's convention adapter deliberately does not build the A2A
//! ingress span: [specification 17.6](../../../docs/plans/rakka-agent/spec.md)
//! makes it "a protocol `SERVER` span that extracts context before durable
//! acceptance", and only the protocol adapter knows when extraction happened.
//! Until slice 6.3a nobody built it, so scenario 21's ingress row had its
//! durable half and no span half, and `AgentOtelSpanKind::Server` was
//! constructed nowhere in the workspace.
//!
//! This module is that half. It maps an ingress segment — which
//! [`crate::agents::RakkaAgentA2AService`] closes unconditionally, so trace
//! continuity does not depend on a feature — into the convention span record.
//! The feature gates the *mapping*, never the propagation: extraction on
//! ingress and injection on egress are unconditional on every path, and gating
//! them would remove trace continuity from a default build.

use rakka_agent::{segment_span, AgentSegmentOperation, AgentTelemetrySegment};
use rakka_agent_workflow::{
    AgentOtelSpanExport, AgentOtelSpanKind, AgentOtlpResult, AgentTelemetryContext,
    AgentTimestampMillis,
};

use crate::auth::A2AOperation;

/// Builds the ingress segment for one bounded A2A operation.
///
/// `telemetry` is the context extracted from the request *before* durable
/// acceptance, which is what makes the accepted command belong to the caller's
/// trace rather than to one invented afterwards
/// ([17.5](../../../docs/plans/rakka-agent/spec.md)). A request whose context
/// was malformed arrives here with the empty context — dropped whole at
/// extraction, never rejected — and maps to no span, because a segment with no
/// trace parent belongs to no trace.
#[must_use]
pub fn a2a_ingress_segment(
    operation: A2AOperation,
    telemetry: &AgentTelemetryContext,
    start: AgentTimestampMillis,
    end: AgentTimestampMillis,
) -> AgentTelemetrySegment {
    AgentTelemetrySegment::new(
        AgentSegmentOperation::A2aIngress {
            operation: operation.as_label().to_string(),
        },
        start,
        end,
    )
    .telemetry(telemetry.clone())
}

/// Maps one ingress segment to its `SERVER` span record.
///
/// The kind is the point: every other row the agent domain emits is
/// `INTERNAL`, `CLIENT`, `PRODUCER`, or `CONSUMER`, and a trace that begins at
/// an A2A request has no server span to root it without this.
pub fn a2a_ingress_span(segment: &AgentTelemetrySegment) -> AgentOtlpResult<AgentOtelSpanExport> {
    let span = segment_span(segment)?;
    debug_assert_eq!(span.kind, AgentOtelSpanKind::Server);
    Ok(span)
}

#[cfg(test)]
mod tests {
    use rakka_agent::AgentSegmentOutcome;

    use super::*;

    const TRACE_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    #[test]
    fn an_ingress_segment_maps_to_a_server_span_naming_its_operation() {
        let telemetry = AgentTelemetryContext {
            trace_parent: Some(TRACE_PARENT.to_string()),
            ..AgentTelemetryContext::default()
        };
        let segment = a2a_ingress_segment(
            A2AOperation::SendMessage,
            &telemetry,
            AgentTimestampMillis::new(1),
            AgentTimestampMillis::new(4),
        );
        assert_eq!(segment.outcome, AgentSegmentOutcome::Unset);

        let span = a2a_ingress_span(&segment.ok()).expect("the ingress span maps");
        assert_eq!(span.kind, AgentOtelSpanKind::Server);
        assert_eq!(span.trace_id, "0af7651916cd43dd8448eb211c80319c");
        assert_eq!(
            span.attributes
                .get("rakka.agent.a2a.operation")
                .map(String::as_str),
            Some(A2AOperation::SendMessage.as_label())
        );
        span.validate().expect("the record is valid");
    }

    #[test]
    fn a_request_whose_context_was_dropped_maps_to_no_span() {
        // Extraction drops a malformed context whole rather than rejecting the
        // request, so this is the shape an unparseable `traceparent` produces.
        let segment = a2a_ingress_segment(
            A2AOperation::CancelTask,
            &AgentTelemetryContext::default(),
            AgentTimestampMillis::new(1),
            AgentTimestampMillis::new(2),
        );
        assert!(
            a2a_ingress_span(&segment).is_err(),
            "a segment with no trace parent belongs to no trace"
        );
    }
}
