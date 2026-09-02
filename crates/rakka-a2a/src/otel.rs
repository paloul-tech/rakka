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
//! [`crate::agents::RakkaAgentA2AService`] closes unconditionally on every
//! entry point, so trace continuity does not depend on a feature — into the
//! convention span record.
//! The feature gates the *mapping*, never the propagation: extraction on
//! ingress and injection on egress are unconditional on every path, and gating
//! them would remove trace continuity from a default build.

// The feature maps the ingress segments `agents` produces, so it must imply
// `agents`. It once did not: the non-`?` form `rakka-agent/otel` activates the
// optional dependency without activating `agents`, so this module compiled
// against a service that was not there — an unresolved doc link was the only
// evidence, and nothing enforces those. Now the inert configuration does not
// build, and `scripts/validate.sh` checks it.
#[cfg(not(feature = "agents"))]
compile_error!(
    "the `otel` feature maps segments the `agents` service closes, so it must imply `agents`"
);

use rakka_agent::{segment_span, AgentTelemetrySegment};
use rakka_agent_workflow::{AgentOtelSpanExport, AgentOtelSpanKind, AgentOtlpResult};

/// Maps one ingress segment to its `SERVER` span record.
///
/// The kind is the point: every other row the agent domain emits is
/// `INTERNAL`, `CLIENT`, `PRODUCER`, or `CONSUMER`, and a trace that begins at
/// an A2A request has no server span to root it without this.
///
/// A deployment does not have to call this. The production path is the segment
/// sink — [`crate::agents::RakkaAgentA2AService::with_segments`] closes an
/// ingress segment on every entry point, ungated, and
/// `rakka_agent::AgentGenAiSpanExporter` maps it — so the `SERVER` span exists
/// whether or not anything calls this function. It is here for an application
/// mapping one segment by hand, and the assertion is what makes the kind a
/// checked claim rather than a comment.
///
/// This module deliberately does **not** build the segment. The vocabulary is
/// ungated and the service owns the single construction site, so a second
/// constructor behind a feature gate would be a copy that drifts — as one did,
/// taking a typed operation while the live path took a label.
pub fn a2a_ingress_span(segment: &AgentTelemetrySegment) -> AgentOtlpResult<AgentOtelSpanExport> {
    let span = segment_span(segment)?;
    debug_assert_eq!(span.kind, AgentOtelSpanKind::Server);
    Ok(span)
}

#[cfg(test)]
mod tests {
    use rakka_agent::{AgentSegmentOperation, AgentSegmentOutcome};
    use rakka_agent_workflow::{AgentTelemetryContext, AgentTimestampMillis};

    use super::*;
    use crate::auth::A2AOperation;

    const TRACE_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    /// The segment the service closes maps to a `SERVER` span naming its
    /// operation.
    ///
    /// Built here the way the service builds it — the ungated vocabulary, with
    /// the class's own label — because that is the only construction there is.
    #[test]
    fn an_ingress_segment_maps_to_a_server_span_naming_its_operation() {
        let telemetry = AgentTelemetryContext {
            trace_parent: Some(TRACE_PARENT.to_string()),
            ..AgentTelemetryContext::default()
        };
        let segment = AgentTelemetrySegment::new(
            AgentSegmentOperation::A2aIngress {
                operation: A2AOperation::SendMessage.as_label().to_string(),
            },
            AgentTimestampMillis::new(1),
            AgentTimestampMillis::new(4),
        )
        .telemetry(telemetry);
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
        let segment = AgentTelemetrySegment::new(
            AgentSegmentOperation::A2aIngress {
                operation: A2AOperation::CancelTask.as_label().to_string(),
            },
            AgentTimestampMillis::new(1),
            AgentTimestampMillis::new(2),
        );
        assert!(
            a2a_ingress_span(&segment).is_err(),
            "a segment with no trace parent belongs to no trace"
        );
    }
}
