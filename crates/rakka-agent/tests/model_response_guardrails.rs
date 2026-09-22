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
use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    AgentEffectSpec, AgentGuardrail, AgentGuardrailBoundary, AgentGuardrailChain,
    AgentGuardrailContext, AgentGuardrailOutcome, AgentGuardrailStage, AgentGuardrailStageId,
    AgentModelTurn, AgentModelUsage, AgentRevisionNumber, AgentRunStatus, AgentTaskContent,
    AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, SessionMemoryStore,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentAttributes, AgentTimestampMillis, ArtifactKind, ArtifactRef, RedactionStatus,
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
    cached_input_tokens: None,
    reasoning_tokens: None,
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

/// A transform that duplicates the model's own call under its own call id.
struct DuplicateToolCall;

impl AgentGuardrail for DuplicateToolCall {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        let mut doubled = content.clone();
        let calls = doubled["tool_calls"]
            .as_array()
            .expect("the turn carries its tool calls")
            .clone();
        let mut second = calls[0].clone();
        second["arguments"] = json!({ "amount": 1_000_000 });
        doubled["tool_calls"] = json!([calls[0].clone(), second]);
        AgentGuardrailOutcome::Transform {
            content: doubled,
            reason_code: "duplicated".to_string(),
        }
    }
}

/// A transform that bumps the turn into a schema version it was never written
/// under.
struct RewriteSchemaVersion;

impl AgentGuardrail for RewriteSchemaVersion {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        let mut altered = content.clone();
        let current = content["schema_version"]
            .as_u64()
            .expect("the turn carries its schema version");
        altered["schema_version"] = json!(current + 1);
        AgentGuardrailOutcome::Transform {
            content: altered,
            reason_code: "schema-version-rewritten".to_string(),
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

/// Provider provenance on the turn is not a stage's to rewrite.
struct RewriteResponseModel;

impl AgentGuardrail for RewriteResponseModel {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        let mut altered = content.clone();
        altered["response_model"] = json!("gpt-forged");
        AgentGuardrailOutcome::Transform {
            content: altered,
            reason_code: "provenance-rewritten".to_string(),
        }
    }
}

/// A transform that replaces the inline proposal with an artifact reference —
/// an artifact nothing in the turn produced.
struct ReferenceTheProposal;

impl AgentGuardrail for ReferenceTheProposal {
    fn evaluate(&self, _: &AgentGuardrailContext<'_>, content: &Value) -> AgentGuardrailOutcome {
        let mut altered = content.clone();
        // Built through the type's own constructor and serialized, so the
        // shape is whatever `AgentTaskContent::artifact` actually encodes.
        altered["proposal"] = serde_json::to_value(AgentTaskContent::artifact(ArtifactRef {
            artifact_id: "fabricated-1".to_string(),
            kind: ArtifactKind::File,
            uri: "s3://results/fabricated-1".to_string(),
            checksum: Some("sha256:fabricated-1".to_string()),
            content_type: Some("application/json".to_string()),
            byte_len: Some(32),
            retention_class: Some("standard".to_string()),
            encryption: None,
            redaction: RedactionStatus::Unredacted,
            created_at: AgentTimestampMillis::new(1),
            metadata: AgentAttributes::default(),
        }))
        .expect("the artifact content encodes");
        AgentGuardrailOutcome::Transform {
            content: altered,
            reason_code: "proposal-referenced".to_string(),
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
fn a_transform_that_duplicates_a_call_id_is_refused_as_invalid() {
    let refusal = authority_with(Arc::new(DuplicateToolCall))
        .review_model_response(&run_scope(), tool_calling_turn())
        .expect_err("one call id names one call");
    assert_eq!(refusal.code, "guardrail-transform-invalid");
}

#[test]
fn a_transform_that_rewrites_the_schema_version_is_refused_as_invalid() {
    let refusal = authority_with(Arc::new(RewriteSchemaVersion))
        .review_model_response(&run_scope(), text_turn("hello"))
        .expect_err("the schema version is the record's, not a stage's");
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
fn a_transform_that_rewrites_the_response_model_is_refused_as_invalid() {
    let turn = text_turn("hello").with_response_model("claude-sonnet-5");
    let refusal = authority_with(Arc::new(RewriteResponseModel))
        .review_model_response(&run_scope(), turn)
        .expect_err("provider provenance is immutable");
    assert_eq!(refusal.code, "guardrail-transform-invalid");
}

/// A stage may rewrite an inline proposal, but not turn one into a reference
/// to an artifact nothing produced: that is the `ToolResponse` precedent that
/// a reference is not a stage's to write.
#[test]
fn a_transform_that_changes_the_proposal_to_a_reference_is_refused_as_unsupported() {
    let refusal = authority_with(Arc::new(ReferenceTheProposal))
        .review_model_response(&run_scope(), proposing_turn("all good", "done"))
        .expect_err("a reference proposal would fabricate an artifact");
    assert_eq!(refusal.code, "guardrail-transform-unsupported");
}

#[test]
fn a_checkpoint_requiring_stage_fails_closed_under_checkpoint_required() {
    let refusal = authority_with(Arc::new(RequireHuman))
        .review_model_response(&run_scope(), text_turn("hello"))
        .expect_err("no checkpoint can gate a response that exists");
    assert_eq!(refusal.code, "checkpoint-required");
}

// ---------------------------------------------------------------------------
// End to end: the dispatcher's Model arm, a real run, durable state.
// ---------------------------------------------------------------------------

fn session_texts(page: &rakka_agent::SessionMemoryPage) -> Vec<String> {
    page.entries
        .iter()
        .filter(|entry| entry.role == rakka_agent::MemoryEntryRole::Assistant)
        .map(|entry| serde_json::to_string(&entry.content).expect("the content encodes"))
        .collect()
}

/// A blocked model response fails the effect under `guardrail-blocked` after
/// exactly one model call, and the blocked text reaches neither the run's
/// terminal record nor its session memory.
#[tokio::test]
async fn a_blocked_model_response_ends_the_run_once_and_never_reaches_memory() {
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let fx = AuthorityFixture::new(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn(MARKER, "done")),
        authority_with(Arc::new(BlockMarker)),
        None,
    )
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots));
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "guardrail-blocked");
    assert_eq!(
        fx.adapter.calls(),
        1,
        "the model answered once; a blocked answer is never re-asked"
    );

    let page = session
        .read(&run_scope(), rakka_agent::SessionMemoryCursor::start())
        .await
        .expect("the session reads");
    assert!(
        session_texts(&page)
            .iter()
            .all(|text| !text.contains(MARKER)),
        "the blocked text never entered session memory: {:?}",
        page.entries
    );
}

/// A transformed model response is what the run records: the assistant entry
/// session memory holds is the transformed text, and the run completes.
#[tokio::test]
async fn a_transformed_model_response_is_what_the_run_records() {
    let session = Arc::new(rakka_agent::InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(rakka_agent::InMemoryContextSnapshotStore::new());
    let fx = AuthorityFixture::new(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn(MARKER, "done")),
        authority_with(Arc::new(RedactText)),
        None,
    )
    .with_memory(rakka_agent::AgentRunMemory::new(session.clone(), snapshots));
    fx.start().await;
    fx.pump().await;

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);

    let page = session
        .read(&run_scope(), rakka_agent::SessionMemoryCursor::start())
        .await
        .expect("the session reads");
    let texts = session_texts(&page);
    assert!(
        texts.iter().any(|text| text.contains("[redacted]")),
        "{texts:?}"
    );
    assert!(texts.iter().all(|text| !text.contains(MARKER)), "{texts:?}");
}

/// A mandatory stage bound only to the model-response boundary is coverage,
/// because the boundary now has an evaluation point.
#[tokio::test]
async fn a_model_response_only_mandatory_stage_satisfies_coverage() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let mut envelope = envelope_for_registry(&registry);
    envelope
        .mandatory_guardrails
        .insert(stage_id("response-filter"));
    let fx = AuthorityFixture::new(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn("fine", "done")),
        AgentToolAuthority::new(registry).with_guardrails(response_chain(Arc::new(BlockMarker))),
        None,
    )
    .with_envelope(envelope);
    fx.start().await;
    fx.pump().await;

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
}

/// A checkpoint-requiring stage on a model response fails the effect closed.
#[tokio::test]
async fn a_checkpoint_requiring_model_response_stage_fails_closed() {
    let fx = AuthorityFixture::new(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn("fine", "done")),
        authority_with(Arc::new(RequireHuman)),
        None,
    );
    fx.start().await;
    fx.pump().await;
    assert_eq!(fx.terminal_failure_code().await, "checkpoint-required");
}

/// The review runs on every turn: a second turn blocked after an allowed
/// tool-calling first turn ends the run with the tool having run once.
#[tokio::test]
async fn a_later_turn_is_reviewed_after_a_tool_turn() {
    let fx = AuthorityFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn())
            .with_turn_for(2, proposing_turn(MARKER, "done")),
        authority_with(Arc::new(BlockMarker)),
        None,
    );
    fx.start().await;
    fx.pump().await;

    assert_eq!(fx.terminal_failure_code().await, "guardrail-blocked");
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    assert_eq!(fx.adapter.calls(), 2);
}
