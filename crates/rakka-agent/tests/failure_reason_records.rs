//! The durable shape of a failure's deciding identity: every record that
//! gains a reason decodes without one, serializes without one exactly as it
//! did before the field existed, and round-trips with one.

mod common;

use common::run_scope;
use rakka_agent::{
    AgentDelegationStatus, AgentEffectSpec, AgentFailureReason, AgentGuardrailStageId,
    AgentHandoffStatus, AgentRevisionNumber, AgentRunEffect, AgentRunEffectOutcome,
    AgentRunEffectRequest, AgentRunTerminalReason, AgentToolCallId, AgentToolCallRequest,
    AgentToolId, AgentUnknownOutcomeEvidence, AGENT_FAILURE_REASON_CODE_MAX_LENGTH,
    AGENT_IDENTITY_MAX_LENGTH,
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
            attempts: None,
            max_attempts: None,
        },
        &AgentRunTerminalReason::EffectFailed {
            effect_id,
            code: "guardrail-blocked".to_string(),
            reason: Some(reason()),
            attempts: None,
            max_attempts: None,
        },
        "effect-failed",
        "reason",
    );
}

/// Issue #100 ask 3: the failing effect's attempt counts are additive on the
/// terminal reason — absent from a record written before them, and from one
/// that has none, and round-tripping when present.
#[test]
fn the_terminal_reasons_attempt_counts_are_additive() {
    let effect_id = effect().effect_id;
    let without = AgentRunTerminalReason::EffectFailed {
        effect_id: effect_id.clone(),
        code: "dispatch-collaborator-failed".to_string(),
        reason: None,
        attempts: None,
        max_attempts: None,
    };
    let with = AgentRunTerminalReason::EffectFailed {
        effect_id,
        code: "dispatch-collaborator-failed".to_string(),
        reason: None,
        attempts: Some(1),
        max_attempts: Some(3),
    };
    let encoded = serde_json::to_value(&without).expect("encodes");
    for field in ["attempts", "max_attempts"] {
        assert!(
            !body(&encoded, "effect-failed").contains_key(field),
            "{field} is not written when it is absent: {encoded}"
        );
    }
    assert_eq!(
        serde_json::from_value::<AgentRunTerminalReason>(encoded).expect("decodes"),
        without
    );
    let encoded = serde_json::to_value(&with).expect("encodes");
    assert_eq!(
        body(&encoded, "effect-failed").get("attempts"),
        Some(&json!(1))
    );
    assert_eq!(
        body(&encoded, "effect-failed").get("max_attempts"),
        Some(&json!(3))
    );
    assert_eq!(
        serde_json::from_value::<AgentRunTerminalReason>(encoded).expect("decodes"),
        with
    );
}

/// Issue #100 ask 4: the definitive marker is additive on the reason — never
/// written when unset, so every reason recorded before it encodes and
/// decodes exactly as it did, and round-tripping when set.
#[test]
fn the_definitive_marker_is_additive() {
    let plain = AgentFailureReason::new("egress-denied").expect("a reason");
    let encoded = serde_json::to_value(&plain).expect("encodes");
    assert_eq!(encoded, json!({"code": "egress-denied"}));
    let decoded: AgentFailureReason = serde_json::from_value(encoded).expect("decodes");
    assert!(!decoded.is_definitive());

    let definitive = AgentFailureReason::definitive("egress-denied");
    let encoded = serde_json::to_value(&definitive).expect("encodes");
    assert_eq!(
        encoded,
        json!({"code": "egress-denied", "definitive": true})
    );
    let decoded: AgentFailureReason = serde_json::from_value(encoded).expect("decodes");
    assert!(decoded.is_definitive());
    assert_eq!(decoded, definitive);
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

/// Issue #100 ask 5: the indeterminate outcome's reason and evidence are
/// additive — an outcome encoded before them decodes with neither, an
/// outcome without them encodes without the keys, and one with them
/// round-trips.
#[test]
fn the_indeterminate_outcomes_evidence_is_additive() {
    let before = json!({
        "indeterminate": {"code": "mcp-outcome-unknown", "message": "lost"}
    });
    let decoded: AgentRunEffectOutcome =
        serde_json::from_value(before.clone()).expect("an older encoding decodes");
    assert_eq!(decoded.failure_reason(), None);
    assert_eq!(decoded.unknown_outcome_evidence(), None);
    assert_eq!(
        serde_json::to_value(&decoded).expect("encodes"),
        before,
        "an outcome without the fields encodes exactly as before"
    );

    let with = AgentRunEffectOutcome::indeterminate("mcp-outcome-unknown", "lost")
        .with_reason(AgentFailureReason::new("mcp-deadline-awaiting-answer"))
        .with_evidence(Some(AgentUnknownOutcomeEvidence::new(
            Some(AgentTimestampMillis::new(4_242)),
            true,
        )));
    let encoded = serde_json::to_value(&with).expect("encodes");
    assert_eq!(
        body(&encoded, "indeterminate").get("reason"),
        Some(&json!({"code": "mcp-deadline-awaiting-answer"}))
    );
    assert_eq!(
        body(&encoded, "indeterminate").get("evidence"),
        Some(&json!({"sent_at": 4_242, "cancellation_issued": true}))
    );
    assert_eq!(
        serde_json::from_value::<AgentRunEffectOutcome>(encoded).expect("decodes"),
        with
    );
}

/// Issue #100 ask 5: evidence with nothing set writes neither key, and an
/// unset cancellation is never written as `false`.
#[test]
fn unset_evidence_fields_are_left_out() {
    let empty = serde_json::to_value(AgentUnknownOutcomeEvidence::default()).expect("encodes");
    assert_eq!(empty, json!({}));
    let sent_only = serde_json::to_value(AgentUnknownOutcomeEvidence::new(
        Some(AgentTimestampMillis::new(7)),
        false,
    ))
    .expect("encodes");
    assert_eq!(sent_only, json!({"sent_at": 7}));
}

/// Issue #100 ask 5: the effect record's evidence is additive, and a new
/// generation does not inherit the superseded generation's evidence.
#[test]
fn the_effect_records_evidence_is_additive_and_cleared_by_a_new_generation() {
    let plain = effect();
    let encoded = serde_json::to_value(&plain).expect("encodes");
    assert!(
        !encoded
            .as_object()
            .expect("an object")
            .contains_key("unknown_outcome"),
        "{encoded}"
    );
    assert_eq!(
        serde_json::from_value::<AgentRunEffect>(encoded).expect("decodes"),
        plain,
        "an effect without evidence decodes back with none"
    );

    let mut parked = effect();
    parked.unknown_outcome = Some(AgentUnknownOutcomeEvidence::new(
        Some(AgentTimestampMillis::new(9)),
        true,
    ));
    let encoded = serde_json::to_value(&parked).expect("encodes");
    assert_eq!(
        encoded.get("unknown_outcome"),
        Some(&json!({"sent_at": 9, "cancellation_issued": true}))
    );
    assert_eq!(
        serde_json::from_value::<AgentRunEffect>(encoded).expect("decodes"),
        parked
    );

    parked
        .begin_next_generation(&run_scope(), AgentTimestampMillis::new(10))
        .expect("the next generation begins");
    assert_eq!(parked.unknown_outcome, None);
}
