//! The durable shape of a failure's deciding identity: every record that
//! gains a reason decodes without one, serializes without one exactly as it
//! did before the field existed, and round-trips with one.

mod common;

use common::run_scope;
use rakka_agent::{
    AgentDelegationStatus, AgentEffectSpec, AgentFailureReason, AgentGuardrailStageId,
    AgentHandoffStatus, AgentRevisionNumber, AgentRunEffect, AgentRunEffectRequest,
    AgentRunTerminalReason, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    AGENT_FAILURE_REASON_CODE_MAX_LENGTH, AGENT_IDENTITY_MAX_LENGTH,
};
use rakka_agent_workflow::AgentTimestampMillis;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};

fn reason() -> AgentFailureReason {
    AgentFailureReason::guardrail(
        AgentGuardrailStageId::new("response-filter").expect("id"),
        "prompt-injection",
    )
}

fn effect() -> AgentRunEffect {
    AgentRunEffect::new(
        &run_scope(),
        1,
        1,
        AgentRunEffectRequest::Tool {
            call: Box::new(
                AgentToolCallRequest::new(
                    AgentToolCallId::new("call-1").expect("id"),
                    AgentToolId::new("charge-card").expect("id"),
                    json!({}),
                )
                .expect("call"),
            ),
        },
        &AgentEffectSpec::read_only(),
        AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(1),
    )
    .expect("the effect derives")
}

/// The object a record's variant serializes to: `{"<tag>": {…}}`.
fn body<'a>(encoded: &'a Value, tag: &str) -> &'a serde_json::Map<String, Value> {
    encoded
        .get(tag)
        .and_then(Value::as_object)
        .unwrap_or_else(|| panic!("expected {tag} in {encoded}"))
}

/// Without a reason: no key is written, and what was written decodes back.
/// With one: it round-trips. Together: a record persisted before the field
/// existed is indistinguishable from one persisted without a reason now.
#[track_caller]
fn assert_additive<T>(without: &T, with: &T, tag: &str, field: &str)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let encoded = serde_json::to_value(without).expect("encodes");
    assert!(
        !body(&encoded, tag).contains_key(field),
        "{field} is not written when it is absent: {encoded}"
    );
    assert_eq!(
        &serde_json::from_value::<T>(encoded).expect("decodes"),
        without
    );
    let encoded = serde_json::to_value(with).expect("encodes");
    assert_eq!(
        body(&encoded, tag).get(field),
        Some(&json!({"code": "prompt-injection", "stage": "response-filter"}))
    );
    assert_eq!(
        &serde_json::from_value::<T>(encoded).expect("decodes"),
        with
    );
}

#[test]
fn the_terminal_reason_is_additive() {
    let effect_id = effect().effect_id;
    assert_additive(
        &AgentRunTerminalReason::EffectFailed {
            effect_id: effect_id.clone(),
            code: "guardrail-blocked".to_string(),
            reason: None,
        },
        &AgentRunTerminalReason::EffectFailed {
            effect_id,
            code: "guardrail-blocked".to_string(),
            reason: Some(reason()),
        },
        "effect-failed",
        "reason",
    );
}

#[test]
fn the_two_cells_are_additive() {
    assert_additive(
        &AgentDelegationStatus::Failed {
            code: "guardrail-blocked".to_string(),
            reason: None,
        },
        &AgentDelegationStatus::Failed {
            code: "guardrail-blocked".to_string(),
            reason: Some(reason()),
        },
        "failed",
        "reason",
    );
    assert_additive(
        &AgentHandoffStatus::Failed {
            code: "guardrail-blocked".to_string(),
            reason: None,
        },
        &AgentHandoffStatus::Failed {
            code: "guardrail-blocked".to_string(),
            reason: Some(reason()),
        },
        "failed",
        "reason",
    );
}

#[test]
fn the_effect_record_is_additive() {
    let plain = effect();
    let encoded = serde_json::to_value(&plain).expect("encodes");
    assert!(
        !encoded
            .as_object()
            .expect("an object")
            .contains_key("last_error_reason"),
        "{encoded}"
    );
    assert_eq!(
        serde_json::from_value::<AgentRunEffect>(encoded).expect("decodes"),
        plain
    );

    let mut failed = effect();
    failed.last_error_code = Some("guardrail-blocked".to_string());
    failed.last_error_reason = Some(reason());
    let encoded = serde_json::to_value(&failed).expect("encodes");
    assert_eq!(
        encoded.get("last_error_reason"),
        Some(&json!({"code": "prompt-injection", "stage": "response-filter"}))
    );
    assert_eq!(
        serde_json::from_value::<AgentRunEffect>(encoded).expect("decodes"),
        failed
    );
}

#[test]
fn a_maximal_reason_is_bounded() {
    let maximal = AgentFailureReason::guardrail(
        AgentGuardrailStageId::new("s".repeat(AGENT_IDENTITY_MAX_LENGTH)).expect("id"),
        "c".repeat(AGENT_FAILURE_REASON_CODE_MAX_LENGTH * 4),
    );
    let bytes = serde_json::to_vec(&maximal).expect("encodes").len();
    assert!(
        bytes <= AGENT_IDENTITY_MAX_LENGTH + AGENT_FAILURE_REASON_CODE_MAX_LENGTH + 32,
        "a reason costs a record at most its two bounds and its keys: {bytes}"
    );
}
