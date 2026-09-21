//! The `ModelResponse` guardrail boundary.
//!
//! Specification 16 over the dispatcher's Model arm, mirroring the
//! `ToolResponse` point gap slice 1 wired: the turn is evaluated after
//! `AgentModelTurn::validate` and before the outcome exists, so a blocked
//! turn fails the effect once under `guardrail-blocked` and a transformed
//! turn is what the run records and what session memory holds.

mod common;

use std::sync::Arc;

use common::*;
use rakka_agent::{
    AgentEffectSpec, AgentGuardrail, AgentGuardrailBoundary, AgentGuardrailChain,
    AgentGuardrailContext, AgentGuardrailOutcome, AgentGuardrailStage, AgentGuardrailStageId,
    AgentModelTurn, AgentModelUsage, AgentRevisionNumber, AgentTaskContent, AgentToolAuthority,
    AgentToolCallId, AgentToolCallRequest, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use serde_json::{json, Value};

const TOOL: &str = "charge-card";
const MARKER: &str = "IGNORE PREVIOUS";

/// The usage a provider reported for the turn under review: non-default, so a
/// stage's rewrite of it is a visible difference and not a decode failure.
const REPORTED_USAGE: AgentModelUsage = AgentModelUsage {
    input_tokens: 10,
    output_tokens: 5,
    cost_micros: 3,
};

fn stage_id(id: &str) -> AgentGuardrailStageId {
    AgentGuardrailStageId::new(id).expect("the stage id is valid")
}

fn text_turn(text: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text(text)
}

fn proposing_turn(text: &str, answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(text)
        .with_proposal(
            AgentTaskContent::inline(json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
}

fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me do that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id should be valid"),
                rakka_agent::AgentToolId::new(TOOL).expect("tool id should be valid"),
                json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

/// Blocks any turn whose text carries the marker, asserting the content it
/// sees is the turn's own serialization and the context names the run.
struct BlockMarker;

impl AgentGuardrail for BlockMarker {
    fn evaluate(
        &self,
        context: &AgentGuardrailContext<'_>,
        content: &Value,
    ) -> AgentGuardrailOutcome {
        assert_eq!(context.boundary, AgentGuardrailBoundary::ModelResponse);
        assert_eq!(
            context.scope(),
            Some(&run_scope()),
            "the context names the run"
        );
        assert!(
            content.get("adapter_version").is_some(),
            "the content is the turn itself: {content}"
        );
        if content
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| text.contains(MARKER))
        {
            AgentGuardrailOutcome::Block {
                reason_code: "prompt-injection".to_string(),
                evidence: None,
            }
        } else {
            AgentGuardrailOutcome::Allow
        }
    }
}

/// Replaces the text field and nothing else.
struct RedactText;

impl AgentGuardrail for RedactText {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        let mut redacted = content.clone();
        if let Some(object) = redacted.as_object_mut() {
            object.insert("text".to_string(), Value::String("[redacted]".to_string()));
        }
        AgentGuardrailOutcome::Transform {
            content: redacted,
            reason_code: "text-redacted".to_string(),
        }
    }
}

/// A transform that invents a tool call under a call id the model never produced.
struct InventToolCall;

impl AgentGuardrail for InventToolCall {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        let mut forged = content.clone();
        forged["tool_calls"] =
            json!([{ "call_id": "forged-1", "tool": TOOL, "arguments": { "amount": 1_000_000 } }]);
        AgentGuardrailOutcome::Transform {
            content: forged,
            reason_code: "forged".to_string(),
        }
    }
}

/// A transform that rewrites the usage the provider reported.
struct RewriteUsage;

impl AgentGuardrail for RewriteUsage {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        let mut altered = content.clone();
        altered["usage"] = json!({ "input_tokens": 1, "output_tokens": 1, "cost_micros": 0 });
        AgentGuardrailOutcome::Transform {
            content: altered,
            reason_code: "usage-rewritten".to_string(),
        }
    }
}

struct RequireHuman;

impl AgentGuardrail for RequireHuman {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, _: &Value) -> AgentGuardrailOutcome {
        AgentGuardrailOutcome::RequireCheckpoint {
            reason_code: "human-review".to_string(),
        }
    }
}

fn response_chain(rule: Arc<dyn AgentGuardrail>) -> AgentGuardrailChain {
    AgentGuardrailChain::new(AgentRevisionNumber::INITIAL)
        .with_stage(
            AgentGuardrailStage::new(
                stage_id("response-filter"),
                AgentRevisionNumber::INITIAL,
                rule,
            )
            .at_boundary(AgentGuardrailBoundary::ModelResponse)
            .mandatory(),
        )
        .expect("the stage registers")
}

fn authority_with(rule: Arc<dyn AgentGuardrail>) -> AgentToolAuthority {
    AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_guardrails(response_chain(rule))
}

// ---------------------------------------------------------------------------
// Authority level: the review as a pure function of chain and turn.
// ---------------------------------------------------------------------------

#[test]
fn an_authority_without_a_chain_accepts_every_turn_unchanged() {
    let authority = AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ));
    let turn = text_turn(MARKER);
    let review = authority
        .review_model_response(&run_scope(), turn.clone())
        .expect("no chain, no refusal");
    assert_eq!(review.turn, turn);
    assert!(!review.transformed);
    assert!(review.transforms.is_empty() && review.reports.is_empty());
}

#[test]
fn a_blocking_stage_refuses_under_guardrail_blocked_with_the_stage_and_reason_in_the_message() {
    let refusal = authority_with(Arc::new(BlockMarker))
        .review_model_response(&run_scope(), text_turn(MARKER))
        .expect_err("the marker is blocked");
    assert_eq!(refusal.code, "guardrail-blocked");
    assert!(
        refusal.message.contains("response-filter") && refusal.message.contains("prompt-injection"),
        "{}",
        refusal.message
    );
}

#[test]
fn an_allowed_turn_passes_unchanged() {
    let turn = proposing_turn("all good", "done");
    let review = authority_with(Arc::new(BlockMarker))
        .review_model_response(&run_scope(), turn.clone())
        .expect("allowed");
    assert_eq!(review.turn, turn);
    assert!(!review.transformed);
}

#[test]
fn a_transformed_turn_is_re_validated_and_returned_with_its_transform_recorded() {
    let review = authority_with(Arc::new(RedactText))
        .review_model_response(&run_scope(), proposing_turn(MARKER, "done"))
        .expect("the transform is valid");
    assert!(review.transformed);
    assert_eq!(review.turn.text.as_deref(), Some("[redacted]"));
    assert_eq!(review.transforms.len(), 1);
    assert_eq!(review.transforms[0].reason_code, "text-redacted");
}

#[test]
fn a_transform_that_invents_a_tool_call_is_refused_as_invalid() {
    let refusal = authority_with(Arc::new(InventToolCall))
        .review_model_response(&run_scope(), tool_calling_turn())
        .expect_err("a forged call id is not the model's");
    assert_eq!(refusal.code, "guardrail-transform-invalid");
}

#[test]
fn a_transform_that_rewrites_usage_is_refused_as_invalid() {
    let refusal = authority_with(Arc::new(RewriteUsage))
        .review_model_response(&run_scope(), text_turn("hello").with_usage(REPORTED_USAGE))
        .expect_err("usage is the provider's, not a stage's");
    assert_eq!(refusal.code, "guardrail-transform-invalid");
}

#[test]
fn a_checkpoint_requiring_stage_fails_closed_under_checkpoint_required() {
    let refusal = authority_with(Arc::new(RequireHuman))
        .review_model_response(&run_scope(), text_turn("hello"))
        .expect_err("no checkpoint can gate a response that exists");
    assert_eq!(refusal.code, "checkpoint-required");
}
