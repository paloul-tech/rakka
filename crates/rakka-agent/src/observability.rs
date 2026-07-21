//! Structured decision events, spans, and metrics.
//!
//! Owns the bounded trace segments with persisted W3C context and links across
//! every durable boundary — no span stays open across a wait — the structured
//! decision events that explain why the runtime did what it did, and the
//! bounded metric set, which never carries an identifier in a label.
//!
//! Content capture is disabled by default. Runtime events are observability and
//! never the correctness source: the durable run, inbox, and outbox state is.
//! An operational answer must therefore stay correct with telemetry entirely
//! unavailable, which is what [`crate::query`] guarantees.
//!
//! Specification: section 17. Filled by slice 1.13, reusing the existing
//! `rakka-agent-workflow` trace-context and OTLP substrate.

use rakka_agent_workflow::{
    validate_agent_span_link, validate_agent_telemetry_context, AgentTelemetryContext,
};

/// Most span links one persisted telemetry context may carry.
///
/// Links accumulate where causality genuinely branches — a regenerated effect
/// links its prior attempt, a resume links the span that parked — and every
/// source of accumulation is already bounded (generations by reconciliation,
/// checkpoints per effect). The cap is a backstop that keeps a durable record
/// bounded even if a caller loops: the *newest* links are kept, because a
/// resume links backwards and the most recent causes are the ones an operator
/// walks first.
pub const AGENT_TELEMETRY_MAX_SPAN_LINKS: usize = 8;

/// Admits a telemetry context to durable state: strict on write, so reads can
/// be permissive.
///
/// Trace context is observability, never correctness
/// ([specification 17.1](../../../docs/plans/rakka-agent/spec.md)), which is
/// why every durable record reads an *absent* context as "nothing recorded"
/// rather than failing closed. That permissiveness is only safe because this
/// gate keeps malformed values out on the way in
/// ([specification 17.5](../../../docs/plans/rakka-agent/spec.md)):
///
/// - a `traceparent`/`tracestate` pair that fails W3C validation is dropped
///   whole, never persisted partially;
/// - each span link is validated independently, so one malformed link does not
///   discard the valid causality next to it;
/// - links are capped at [`AGENT_TELEMETRY_MAX_SPAN_LINKS`], keeping the
///   newest; and
/// - baggage is cleared unconditionally: M1 persists no baggage
///   ([specification 17.15](../../../docs/plans/rakka-agent/spec.md); slice
///   1.13 resolution), and externally received baggage is untrusted.
///
/// The function is total — it returns whatever bounded, valid subset the input
/// held, down to the empty context — because a boundary that *refused* a
/// command over its telemetry would make observability a correctness input.
#[must_use]
pub fn sanitize_agent_telemetry_context(context: AgentTelemetryContext) -> AgentTelemetryContext {
    let mut sanitized = AgentTelemetryContext::default();

    let trace_candidate = AgentTelemetryContext {
        trace_parent: context.trace_parent,
        trace_state: context.trace_state,
        ..AgentTelemetryContext::default()
    };
    if validate_agent_telemetry_context(&trace_candidate).is_ok() {
        sanitized.trace_parent = trace_candidate.trace_parent;
        sanitized.trace_state = trace_candidate.trace_state;
    }

    let mut links: Vec<_> = context
        .span_links
        .into_iter()
        .filter(|link| validate_agent_span_link(link).is_ok())
        .collect();
    if links.len() > AGENT_TELEMETRY_MAX_SPAN_LINKS {
        links.drain(..links.len() - AGENT_TELEMETRY_MAX_SPAN_LINKS);
    }
    sanitized.span_links = links;

    sanitized
}

#[cfg(test)]
mod tests {
    use rakka_agent_workflow::{AgentAttributes, AgentSpanLink};

    use super::*;

    const TRACE_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    fn link(span_id: &str) -> AgentSpanLink {
        AgentSpanLink {
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            span_id: span_id.to_string(),
            trace_state: None,
            attributes: AgentAttributes::new(),
        }
    }

    #[test]
    fn a_valid_context_survives_with_its_baggage_cleared() {
        let mut context = AgentTelemetryContext {
            trace_parent: Some(TRACE_PARENT.to_string()),
            trace_state: Some("vendor=value".to_string()),
            ..AgentTelemetryContext::default()
        };
        context
            .baggage
            .insert("tenant".to_string(), "acme".to_string());
        context.span_links.push(link("00f067aa0ba902b7"));

        let sanitized = sanitize_agent_telemetry_context(context);

        assert_eq!(sanitized.trace_parent.as_deref(), Some(TRACE_PARENT));
        assert_eq!(sanitized.trace_state.as_deref(), Some("vendor=value"));
        assert_eq!(sanitized.span_links.len(), 1);
        assert!(sanitized.baggage.is_empty(), "M1 persists no baggage");
    }

    #[test]
    fn a_malformed_trace_parent_is_dropped_whole_without_touching_valid_links() {
        let context = AgentTelemetryContext {
            trace_parent: Some("not-a-traceparent".to_string()),
            trace_state: Some("vendor=value".to_string()),
            span_links: vec![link("00f067aa0ba902b7")],
            ..AgentTelemetryContext::default()
        };

        let sanitized = sanitize_agent_telemetry_context(context);

        assert!(sanitized.trace_parent.is_none());
        assert!(
            sanitized.trace_state.is_none(),
            "tracestate must not outlive the traceparent it rode with"
        );
        assert_eq!(sanitized.span_links.len(), 1);
    }

    #[test]
    fn a_malformed_link_is_filtered_while_the_rest_survive() {
        let mut bad = link("00f067aa0ba902b7");
        bad.span_id = "short".to_string();
        let context = AgentTelemetryContext {
            span_links: vec![bad, link("00f067aa0ba902b8")],
            ..AgentTelemetryContext::default()
        };

        let sanitized = sanitize_agent_telemetry_context(context);

        assert_eq!(sanitized.span_links.len(), 1);
        assert_eq!(sanitized.span_links[0].span_id, "00f067aa0ba902b8");
    }

    #[test]
    fn links_are_capped_keeping_the_newest() {
        let links: Vec<_> = (0..AGENT_TELEMETRY_MAX_SPAN_LINKS + 3)
            .map(|index| link(&format!("00f067aa0ba9{index:04x}")))
            .collect();
        let newest = links.last().expect("links are non-empty").clone();
        let context = AgentTelemetryContext {
            span_links: links,
            ..AgentTelemetryContext::default()
        };

        let sanitized = sanitize_agent_telemetry_context(context);

        assert_eq!(sanitized.span_links.len(), AGENT_TELEMETRY_MAX_SPAN_LINKS);
        assert_eq!(
            sanitized.span_links.last(),
            Some(&newest),
            "the newest links are the ones a resume walks first"
        );
    }

    #[test]
    fn the_empty_context_is_a_fixed_point() {
        assert_eq!(
            sanitize_agent_telemetry_context(AgentTelemetryContext::default()),
            AgentTelemetryContext::default()
        );
    }
}
