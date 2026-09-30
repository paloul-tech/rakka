//! The wire shape of one [`rakka_agent::AgentToolBinding`].
//!
//! A binding is trusted deployment data that can arrive encoded — from
//! configuration, or from the discovered tool list of a remote server — so it
//! crosses a trust boundary on the way in. Decoding therefore re-validates
//! through the same checks construction runs, and a round trip must carry
//! every field rather than defaulting one back to its fail-safe value.

use rakka_agent::{
    AgentCredentialBindingRef, AgentEffectSafetyClass, AgentEnvironmentConcurrencyProtocol,
    AgentGuardrailStageId, AgentReconciliationProtocolRef, AgentToolBinding, AgentToolDeclaration,
    AgentToolKind,
};

mod common;
use common::tool_descriptor_of_kind;

/// A binding carrying every optional field, so a round trip proves each one
/// survives rather than being silently defaulted back to the fail-safe value.
fn populated_binding() -> AgentToolBinding {
    AgentToolBinding::new(
        tool_descriptor_of_kind("mcp.crm.charge", AgentToolKind::RemoteMcp),
        AgentToolDeclaration::new(AgentEffectSafetyClass::Reconcileable).with_credential_binding(
            AgentCredentialBindingRef::new("crm-token").expect("credential binding"),
        ),
        3,
    )
    .with_reconciliation_protocol(
        AgentReconciliationProtocolRef::new("payment-ledger").expect("protocol ref"),
    )
    .with_environment_concurrency(AgentEnvironmentConcurrencyProtocol::Reconciliation)
    .with_timeout_ms(5_000)
    .with_guardrail(AgentGuardrailStageId::new("pii-redaction").expect("stage id"))
    .with_checkpoint_required()
    .with_authorization_required()
}

#[test]
fn every_field_of_a_binding_survives_a_round_trip() {
    let binding = populated_binding();
    let decoded: AgentToolBinding =
        serde_json::from_str(&serde_json::to_string(&binding).expect("encodes")).expect("decodes");
    assert_eq!(decoded, binding);
    assert_eq!(
        decoded
            .effect_spec()
            .expect("the decoded binding projects a spec"),
        binding.effect_spec().expect("the spec projects"),
        "the attempt policy the decoded binding dispatches under is the one that was encoded"
    );
}

#[test]
fn a_binding_round_trips_through_serde_and_is_revalidated_on_decode() {
    let binding = AgentToolBinding::unclassified(tool_descriptor_of_kind(
        "mcp.crm.search",
        AgentToolKind::RemoteMcp,
    ));
    let encoded = serde_json::to_string(&binding).expect("encodes");
    let decoded: AgentToolBinding = serde_json::from_str(&encoded).expect("decodes");
    assert_eq!(decoded, binding);
    let tampered = encoded.replace("\"max_attempts\":1", "\"max_attempts\":0");
    assert!(tampered != encoded, "the fixture must hit the field");
    assert!(
        serde_json::from_str::<AgentToolBinding>(&tampered).is_err(),
        "a decoded binding is validated"
    );
}
