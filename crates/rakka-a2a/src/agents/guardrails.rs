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
/// In a transformed view, a collaboration key the stage omitted leaves that
/// field unchanged and an explicit `null` clears it; a field the original did
/// not carry may not be added.
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
        // An omitted key means *unchanged*, never cleared: a stage that
        // rewrites the parts and returns the collaboration object without a
        // key it did not care about must not silently erase that field —
        // which for a required one would surface downstream as a missing
        // field and blame the caller. Clearing stays available, spelled as an
        // explicit `null`.
        let field = |name: &str, original: Option<&String>| match collaboration.get(name) {
            None => Ok(original.cloned()),
            Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.clone())),
            Some(_) => Err(invalid(&format!(
                "the collaboration field {name} is neither a string nor null"
            ))),
        };
        let new_text = A2aCollaborationText {
            body: field("body", text.body.as_ref())?,
            reason: field("reason", text.reason.as_ref())?,
            context: match collaboration.get("context") {
                None => text.context.clone(),
                Some(Value::Null) => Vec::new(),
                Some(Value::Array(items)) => items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect(),
                Some(_) => {
                    return Err(invalid(
                        "the collaboration context is neither an array nor null",
                    ))
                }
            },
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use a2a::PartContent;
    use rakka_agent::{
        AgentGuardrail, AgentGuardrailOutcome, AgentGuardrailStage, AgentGuardrailStageId,
        AgentRevisionNumber, AgentTaskId, AgentTaskScope, TenantId,
    };

    use super::*;

    /// Replaces the parts and writes the scripted collaboration object back
    /// verbatim, so a test says exactly which keys a stage returned.
    struct ScriptedCollaboration(Value);

    impl AgentGuardrail for ScriptedCollaboration {
        fn evaluate(
            &self,
            _: &AgentGuardrailContext<'_>,
            content: &Value,
        ) -> AgentGuardrailOutcome {
            let mut transformed = content.clone();
            transformed["parts"] = json!([{ "kind": "text", "text": "[redacted]" }]);
            transformed["collaboration"] = self.0.clone();
            AgentGuardrailOutcome::Transform {
                content: transformed,
                reason_code: "scripted".to_string(),
            }
        }
    }

    fn chain(collaboration: Value) -> AgentGuardrailChain {
        AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
            .with_stage(
                AgentGuardrailStage::new(
                    AgentGuardrailStageId::new("scripted").expect("the stage id is valid"),
                    AgentRevisionNumber::INITIAL,
                    Arc::new(ScriptedCollaboration(collaboration)),
                )
                .at_boundary(AgentGuardrailBoundary::A2aIngress),
            )
            .expect("the stage registers")
    }

    fn original() -> A2aCollaborationText {
        A2aCollaborationText {
            body: Some("the body".to_string()),
            reason: Some("the reason".to_string()),
            context: vec!["line one".to_string(), "line two".to_string()],
        }
    }

    fn parts() -> Vec<Part> {
        vec![Part {
            content: PartContent::Text("the ticket".to_string()),
            filename: None,
            media_type: None,
            metadata: None,
        }]
    }

    fn review(collaboration: Value) -> Result<A2aContentReview, AgentAuthorityRefusal> {
        let scope = AgentTaskScope::new(
            TenantId::new("acme"),
            AgentTaskId::new("task-1").expect("the task id is valid"),
        )
        .expect("the task scope is valid");
        let text = original();
        evaluate_a2a_content(
            &chain(collaboration),
            AgentGuardrailBoundary::A2aIngress,
            AgentGuardrailSubject::Task {
                scope: &scope,
                agent: None,
            },
            &parts(),
            Some(&text),
        )
    }

    fn admitted(collaboration: Value) -> A2aCollaborationText {
        review(collaboration)
            .expect("the transform is admissible")
            .text
            .expect("the review carries the cluster text")
    }

    #[test]
    fn an_omitted_collaboration_key_keeps_the_original_value() {
        // The stage rewrote only the body; it named neither `reason` nor
        // `context`, so both survive unchanged.
        let text = admitted(json!({ "body": "[redacted]" }));
        assert_eq!(text.body.as_deref(), Some("[redacted]"));
        assert_eq!(text.reason, original().reason);
        assert_eq!(text.context, original().context);
    }

    #[test]
    fn an_explicit_null_clears_the_field_an_omitted_key_would_have_kept() {
        let text = admitted(json!({ "body": null, "reason": null }));
        assert_eq!(text.body, None);
        assert_eq!(text.reason, None);
        assert_eq!(
            text.context,
            original().context,
            "the omitted key is still unchanged"
        );
    }

    #[test]
    fn an_explicit_null_or_empty_context_clears_the_lines() {
        assert!(admitted(json!({ "context": null })).context.is_empty());
        assert!(admitted(json!({ "context": [] })).context.is_empty());
    }

    #[test]
    fn a_rewritten_context_replaces_the_lines() {
        assert_eq!(
            admitted(json!({ "context": ["only line"] })).context,
            vec!["only line".to_string()]
        );
    }

    #[test]
    fn a_field_the_original_did_not_carry_may_not_be_added() {
        let scope = AgentTaskScope::new(
            TenantId::new("acme"),
            AgentTaskId::new("task-1").expect("the task id is valid"),
        )
        .expect("the task scope is valid");
        let text = A2aCollaborationText {
            body: None,
            ..original()
        };
        let refusal = evaluate_a2a_content(
            &chain(json!({ "body": "invented" })),
            AgentGuardrailBoundary::A2aIngress,
            AgentGuardrailSubject::Task {
                scope: &scope,
                agent: None,
            },
            &parts(),
            Some(&text),
        )
        .expect_err("a stage may not add a field the message did not carry");
        assert_eq!(refusal.code, "guardrail-transform-invalid");
    }

    #[test]
    fn a_collaboration_field_of_the_wrong_shape_is_refused_rather_than_erased() {
        assert_eq!(
            review(json!({ "body": 7 }))
                .expect_err("a number is neither a string nor null")
                .code,
            "guardrail-transform-invalid"
        );
        assert_eq!(
            review(json!({ "context": "line" }))
                .expect_err("a string is neither an array nor null")
                .code,
            "guardrail-transform-invalid"
        );
    }
}
