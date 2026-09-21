//! The A2A ingress and egress guardrail evaluation, shared by the service's
//! authorized leaves (ingress) and the two in-process send executors (egress).
//!
//! The content a stage sees is a bounded view:
//! `{ "kind": "<boundary>", "parts": [...], "collaboration": { "body", "reason", "context" } }`
//! — the message's parts as the SDK encodes them, and the free-text fields of
//! a collaboration cluster where one is carried, because a conversation
//! turn's text rides the cluster rather than the parts. A transform may
//! rewrite the parts and those text fields and nothing else; parts over the
//! content bound are digested for evaluation and cannot be transformed.

use a2a::Part;
use serde_json::{json, Value};

use rakka_agent::{
    refuse_guardrail_disposition, AgentAuthorityRefusal, AgentContentDigest,
    AgentGuardrailBoundary, AgentGuardrailChain, AgentGuardrailContext, AgentGuardrailReport,
    AgentGuardrailSubject, AgentGuardrailTransform, AGENT_GUARDRAIL_CONTENT_MAX_BYTES,
};

use super::collaboration::AgentCollaborationEnvelope;
use super::ingress::NormalizedAgentCommand;

/// The free-text fields of a collaboration cluster a stage may read and rewrite.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct A2aCollaborationText {
    /// The cluster's free-text body, where its shape carries one.
    pub(crate) body: Option<String>,
    /// The cluster's free-text reason, where its shape carries one.
    pub(crate) reason: Option<String>,
    /// The handoff cluster's bounded context lines.
    pub(crate) context: Vec<String>,
}

/// What the chain decided about one message: replacements only where a
/// stage transformed, and the findings for the trace.
#[derive(Debug, Clone)]
pub(crate) struct A2aContentReview {
    /// The admitted parts, when a stage rewrote them.
    pub(crate) parts: Option<Vec<Part>>,
    /// The admitted cluster text, when a stage rewrote it.
    pub(crate) text: Option<A2aCollaborationText>,
    /// Every applied transform, in stage order.
    pub(crate) transforms: Vec<AgentGuardrailTransform>,
    /// Every report-only finding, in stage order.
    pub(crate) reports: Vec<AgentGuardrailReport>,
}

fn invalid(reason: &str) -> AgentAuthorityRefusal {
    AgentAuthorityRefusal::of(
        "guardrail-transform-invalid",
        format!("the transformed A2A message view is not admissible: {reason}"),
    )
}

/// The cluster text an inbound command carries, when it carries any.
pub(crate) fn collaboration_text(
    envelope: Option<&AgentCollaborationEnvelope>,
) -> Option<A2aCollaborationText> {
    match envelope? {
        AgentCollaborationEnvelope::Team(cluster) => Some(A2aCollaborationText {
            body: cluster.body.clone(),
            ..A2aCollaborationText::default()
        }),
        AgentCollaborationEnvelope::Conversation(cluster) => Some(A2aCollaborationText {
            body: cluster.body.clone(),
            reason: cluster.reason.clone(),
            context: Vec::new(),
        }),
        AgentCollaborationEnvelope::Handoff(cluster) => Some(A2aCollaborationText {
            body: None,
            reason: Some(cluster.reason.clone()),
            context: cluster.context.clone(),
        }),
        AgentCollaborationEnvelope::Delegation(_) => None,
    }
}

/// Writes transformed cluster text back into a normalized command.
pub(crate) fn apply_collaboration_text(
    normalized: &mut NormalizedAgentCommand,
    text: A2aCollaborationText,
) {
    match normalized.collaboration.as_mut() {
        Some(AgentCollaborationEnvelope::Team(cluster)) => cluster.body = text.body,
        Some(AgentCollaborationEnvelope::Conversation(cluster)) => {
            cluster.body = text.body;
            cluster.reason = text.reason;
        }
        Some(AgentCollaborationEnvelope::Handoff(cluster)) => {
            if let Some(reason) = text.reason {
                cluster.reason = reason;
            }
            cluster.context = text.context;
        }
        Some(AgentCollaborationEnvelope::Delegation(_)) | None => {}
    }
}

/// Evaluates the chain over one message at an A2A boundary.
///
/// # Errors
///
/// `guardrail-blocked` for a block, `checkpoint-required` for a checkpoint
/// requirement (nothing at an A2A boundary can satisfy one),
/// `guardrail-content-unencodable` when the parts do not encode,
/// `guardrail-transform-unsupported` for a transform of digested parts, and
/// `guardrail-transform-invalid` for a transform that does not decode to
/// parts, drops them, or touches anything but parts and cluster text.
pub(crate) fn evaluate_a2a_content(
    chain: &AgentGuardrailChain,
    boundary: AgentGuardrailBoundary,
    subject: AgentGuardrailSubject<'_>,
    parts: &[Part],
    text: Option<&A2aCollaborationText>,
) -> Result<A2aContentReview, AgentAuthorityRefusal> {
    let parts_value = serde_json::to_value(parts).map_err(|error| {
        AgentAuthorityRefusal::of(
            "guardrail-content-unencodable",
            format!("the message parts do not encode: {error}"),
        )
    })?;
    let encoded = serde_json::to_vec(&parts_value)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX);
    let truncated = encoded > AGENT_GUARDRAIL_CONTENT_MAX_BYTES;
    let mut view = json!({ "kind": boundary.as_label() });
    if truncated {
        view["parts_truncated"] = json!({
            "bytes": encoded,
            "digest": AgentContentDigest::of_json(&parts_value).to_string(),
        });
    } else {
        view["parts"] = parts_value;
    }
    if let Some(text) = text {
        view["collaboration"] = json!({
            "body": text.body,
            "reason": text.reason,
            "context": text.context,
        });
    }

    let context = AgentGuardrailContext::for_subject(boundary, subject);
    let decision = chain.evaluate(&context, &view);
    let what = match boundary {
        AgentGuardrailBoundary::A2aIngress => "the inbound A2A message",
        _ => "the outbound A2A message",
    };
    refuse_guardrail_disposition(&decision.disposition, what, false)?;

    let mut review = A2aContentReview {
        parts: None,
        text: None,
        transforms: decision.transforms,
        reports: decision.reports,
    };
    if !decision.transformed {
        return Ok(review);
    }
    if truncated {
        return Err(AgentAuthorityRefusal::of(
            "guardrail-transform-unsupported",
            "a guardrail stage transformed a message whose parts were digested for evaluation; \
             digested content cannot be rewritten",
        ));
    }
    let object = decision
        .content
        .as_object()
        .ok_or_else(|| invalid("the view is not an object"))?;
    for key in object.keys() {
        if !matches!(key.as_str(), "kind" | "parts" | "collaboration") {
            return Err(invalid(&format!("it carries an unknown key {key}")));
        }
    }
    let new_parts: Vec<Part> = serde_json::from_value(
        object
            .get("parts")
            .cloned()
            .ok_or_else(|| invalid("it dropped the parts"))?,
    )
    .map_err(|error| invalid(&format!("the parts do not decode: {error}")))?;
    if new_parts.is_empty() {
        return Err(invalid("it carries no parts"));
    }
    review.parts = Some(new_parts);
    if let Some(text) = text {
        let collaboration = object
            .get("collaboration")
            .ok_or_else(|| invalid("it dropped the collaboration text"))?;
        let field = |name: &str| {
            collaboration
                .get(name)
                .and_then(Value::as_str)
                .map(str::to_string)
        };
        let new_text = A2aCollaborationText {
            body: field("body"),
            reason: field("reason"),
            context: collaboration
                .get("context")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
        };
        if (text.body.is_none() && new_text.body.is_some())
            || (text.reason.is_none() && new_text.reason.is_some())
        {
            return Err(invalid(
                "it adds a collaboration field the message did not carry",
            ));
        }
        review.text = Some(new_text);
    }
    Ok(review)
}

/// Logs a review's findings the way the dispatcher logs a tool-response review.
pub(crate) fn log_review(review: &A2aContentReview, what: &str) {
    for transform in &review.transforms {
        tracing::info!(
            stage = %transform.stage,
            stage_revision = %transform.revision,
            reason_code = %transform.reason_code,
            what = what,
            "guardrail transform applied"
        );
    }
    for report in &review.reports {
        tracing::info!(
            stage = %report.stage,
            stage_revision = %report.revision,
            reason_code = %report.reason_code,
            what = what,
            "guardrail report-only finding"
        );
    }
}
