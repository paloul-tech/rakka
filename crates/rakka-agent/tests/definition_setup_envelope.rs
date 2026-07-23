//! Definition, settings, and setup envelope contracts.
//!
//! Specification: sections 7.1 through 7.3. Section 7.3 lists six things a setup
//! or later settings revision must never do — introduce an undeclared tool,
//! weaken a mandatory guardrail, choose an unapproved model, widen credential or
//! knowledge access, add an unauthorized peer, or downgrade effect safety — and
//! there is a test here for each one, keyed on its stable reason code.
//!
//! Enforcement at dispatch lands in slice 1.8. This file proves the contract
//! itself: a widening setup is not constructible.

use rakka_agent::{
    effective_settings_for_turn, AgentAuthorityEnvelope, AgentBudgetCeilings, AgentCapabilityId,
    AgentCoordinationCapabilityKind, AgentCredentialBindingRef, AgentDefinition,
    AgentDefinitionRevision, AgentEffectSafetyClass, AgentEnvironmentRef, AgentExecutionPolicyRef,
    AgentGuardrailStageId, AgentId, AgentModelProfileId, AgentOperationClass, AgentPolicyRef,
    AgentRevisionNumber, AgentRevisionProvenance, AgentSamplingSettings, AgentSettings,
    AgentSettingsChange, AgentSetupRevision, AgentTaskDefinitionId, AgentToolDeclaration,
    AgentToolId, AgentWorkflowToolId, KnowledgeSpaceId, SettingsRevision, SettingsTimingClass,
    AGENT_DESCRIPTION_MAX_LENGTH, AGENT_SETTINGS_MAX_CHANGES,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef, StateSchemaVersion,
};

fn provenance(at: u64) -> AgentRevisionProvenance {
    AgentRevisionProvenance {
        principal: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "operator-1".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

fn tool(id: &str) -> AgentToolId {
    AgentToolId::new(id).expect("tool id should be valid")
}

fn model(id: &str) -> AgentModelProfileId {
    AgentModelProfileId::new(id).expect("model profile id should be valid")
}

fn capability(id: &str) -> AgentCapabilityId {
    AgentCapabilityId::new(id).expect("capability id should be valid")
}

fn credential(id: &str) -> AgentCredentialBindingRef {
    AgentCredentialBindingRef::new(id).expect("credential binding ref should be valid")
}

/// The authority a definition grants: two tools of different safety classes, two
/// approved models, one peer, one knowledge space, one mandatory guardrail, and
/// bounded budgets.
fn granted_envelope() -> AgentAuthorityEnvelope {
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope.tools.insert(
        tool("search"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)
            .with_capability(capability("net.read")),
    );
    envelope.tools.insert(
        tool("charge-card"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::NonIdempotent)
            .with_capability(capability("payments.write"))
            .with_credential_binding(credential("stripe-live")),
    );
    envelope.model_profiles.insert(model("frontier"));
    envelope.model_profiles.insert(model("mini"));
    envelope
        .workflow_tools
        .insert(AgentWorkflowToolId::new("refund-flow").expect("workflow tool id should be valid"));
    envelope
        .task_definitions
        .insert(AgentTaskDefinitionId::new("support-ticket").expect("task definition id"));
    envelope
        .collaborators
        .insert(AgentId::new("billing-agent").expect("agent id should be valid"));
    envelope
        .knowledge_spaces
        .insert(KnowledgeSpaceId::new("support-kb").expect("knowledge space id should be valid"));
    envelope
        .environments
        .insert(AgentEnvironmentRef::new("crm").expect("environment ref should be valid"));
    envelope
        .credential_bindings
        .insert(credential("stripe-live"));
    envelope
        .coordination_capabilities
        .insert(AgentCoordinationCapabilityKind::Handoff);
    envelope
        .operation_classes
        .insert(AgentOperationClass::Interactive);
    envelope.mandatory_guardrails.insert(
        AgentGuardrailStageId::new("pii-redaction").expect("guardrail stage id should be valid"),
    );
    envelope.budgets = AgentBudgetCeilings {
        max_loop_iterations: Some(20),
        max_model_calls: Some(10),
        max_tokens: Some(100_000),
        max_cost_micros: Some(5_000_000),
        ..AgentBudgetCeilings::unbounded()
    };
    envelope
}

fn definition() -> AgentDefinitionRevision {
    let definition = AgentDefinition::new(
        rakka_agent::AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
        "Resolves customer support tickets end to end.",
        granted_envelope(),
    )
    .expect("definition should be valid");
    AgentDefinitionRevision::initial(definition, provenance(1))
}

/// A legal narrowing: fewer tools, one model, no peers, tighter budgets, and the
/// mandatory guardrail preserved.
fn narrowed_envelope() -> AgentAuthorityEnvelope {
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope.tools.insert(
        tool("search"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)
            .with_capability(capability("net.read")),
    );
    envelope.model_profiles.insert(model("mini"));
    envelope
        .operation_classes
        .insert(AgentOperationClass::Interactive);
    envelope.mandatory_guardrails.insert(
        AgentGuardrailStageId::new("pii-redaction").expect("guardrail stage id should be valid"),
    );
    envelope.budgets = AgentBudgetCeilings {
        max_loop_iterations: Some(5),
        max_model_calls: Some(3),
        max_tokens: Some(10_000),
        max_cost_micros: Some(1_000_000),
        ..AgentBudgetCeilings::unbounded()
    };
    envelope
}

fn setup_violation_codes(envelope: AgentAuthorityEnvelope) -> Vec<&'static str> {
    let error = AgentSetupRevision::new(
        AgentRevisionNumber::INITIAL,
        &definition(),
        envelope,
        provenance(2),
    )
    .expect_err("a widening setup must be rejected");
    assert_eq!(error.code(), "envelope-widened");
    error.violation_codes()
}

#[test]
fn a_narrowing_setup_is_accepted() {
    let setup = AgentSetupRevision::new(
        AgentRevisionNumber::INITIAL,
        &definition(),
        narrowed_envelope(),
        provenance(2),
    )
    .expect("a narrowing setup should be accepted");

    assert_eq!(setup.definition_revision(), AgentRevisionNumber::INITIAL);
    assert_eq!(setup.envelope().tools.len(), 1);
    assert!(granted_envelope()
        .narrowing_violations(setup.envelope())
        .is_empty());
}

#[test]
fn a_setup_may_not_introduce_an_undeclared_tool() {
    let mut envelope = narrowed_envelope();
    envelope.tools.insert(
        tool("delete-account"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::NonIdempotent),
    );
    assert_eq!(setup_violation_codes(envelope), vec!["undeclared-tool"]);
}

#[test]
fn a_setup_may_not_widen_a_tool_capability() {
    let mut envelope = narrowed_envelope();
    envelope.tools.insert(
        tool("search"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)
            .with_capability(capability("net.read"))
            .with_capability(capability("net.write")),
    );
    assert_eq!(
        setup_violation_codes(envelope),
        vec!["widened-tool-capability"]
    );
}

#[test]
fn a_setup_may_not_downgrade_effect_safety() {
    // Re-labelling the non-idempotent payment tool as idempotent would make an
    // unsafe retry after an ambiguous attempt look legal.
    let mut envelope = narrowed_envelope();
    envelope.tools.insert(
        tool("charge-card"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::Idempotent)
            .with_capability(capability("payments.write"))
            .with_credential_binding(credential("stripe-live")),
    );
    assert_eq!(
        setup_violation_codes(envelope),
        vec!["downgraded-effect-safety"]
    );
}

#[test]
fn a_setup_may_raise_effect_safety() {
    // The inverse is allowed: a setup may treat a tool as more dangerous than the
    // definition declared, which only ever adds gating.
    let mut envelope = narrowed_envelope();
    envelope.tools.insert(
        tool("search"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::Reconcileable)
            .with_capability(capability("net.read")),
    );
    assert!(granted_envelope()
        .narrowing_violations(&envelope)
        .is_empty());
}

#[test]
fn a_setup_may_not_choose_an_unapproved_model() {
    let mut envelope = narrowed_envelope();
    envelope.model_profiles.insert(model("unreviewed-preview"));
    assert_eq!(
        setup_violation_codes(envelope),
        vec!["unapproved-model-profile"]
    );
}

#[test]
fn a_setup_may_not_widen_credential_or_knowledge_access() {
    let mut envelope = narrowed_envelope();
    envelope
        .credential_bindings
        .insert(credential("hr-database"));
    envelope
        .knowledge_spaces
        .insert(KnowledgeSpaceId::new("finance-kb").expect("knowledge space id should be valid"));
    envelope
        .environments
        .insert(AgentEnvironmentRef::new("prod-cluster").expect("environment ref should be valid"));

    let codes = setup_violation_codes(envelope);
    assert!(codes.contains(&"widened-credential-access"), "{codes:?}");
    assert!(codes.contains(&"widened-knowledge-access"), "{codes:?}");
    assert!(codes.contains(&"widened-environment-access"), "{codes:?}");
}

#[test]
fn a_setup_may_not_add_an_unauthorized_peer() {
    let mut envelope = narrowed_envelope();
    envelope
        .collaborators
        .insert(AgentId::new("payroll-agent").expect("agent id should be valid"));
    assert_eq!(
        setup_violation_codes(envelope),
        vec!["unauthorized-collaborator"]
    );
}

#[test]
fn a_setup_may_not_reroute_or_drop_a_tool_execution_policy() {
    // The execution policy is an opaque application reference: the narrowing
    // check cannot rank "sandboxed" against "host-shell", so neither a
    // substitute nor the policy's absence can be proven stricter. The only
    // legal narrowing keeps the declared routing exactly.
    let sandboxed =
        AgentExecutionPolicyRef::new("sandboxed").expect("execution policy ref should be valid");
    let permissive =
        AgentExecutionPolicyRef::new("host-shell").expect("execution policy ref should be valid");

    let mut granted = AgentAuthorityEnvelope::empty();
    granted.tools.insert(
        tool("deploy"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::Reconcileable)
            .with_execution_policy(sandboxed.clone()),
    );

    // Keeping the declared routing is legal.
    assert!(granted.narrowing_violations(&granted.clone()).is_empty());

    // Substituting another policy is rejected.
    let mut rerouted = granted.clone();
    rerouted.tools.insert(
        tool("deploy"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::Reconcileable)
            .with_execution_policy(permissive.clone()),
    );
    let violations = granted.narrowing_violations(&rerouted);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert_eq!(
        violations[0].reason_code(),
        "rerouted-tool-execution-policy"
    );

    // Dropping the policy is rejected too: absence may route the dispatch
    // through a weaker default.
    let mut dropped = granted.clone();
    dropped.tools.insert(
        tool("deploy"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::Reconcileable),
    );
    let violations = granted.narrowing_violations(&dropped);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert_eq!(
        violations[0].reason_code(),
        "rerouted-tool-execution-policy"
    );

    // And a setup may not attach a policy to a tool the definition routes
    // through none.
    let mut unrouted = AgentAuthorityEnvelope::empty();
    unrouted.tools.insert(
        tool("deploy"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::Reconcileable),
    );
    let mut attached = unrouted.clone();
    attached.tools.insert(
        tool("deploy"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::Reconcileable)
            .with_execution_policy(permissive),
    );
    let violations = unrouted.narrowing_violations(&attached);
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert_eq!(
        violations[0].reason_code(),
        "rerouted-tool-execution-policy"
    );
}

#[test]
fn a_setup_may_not_weaken_a_mandatory_guardrail() {
    let mut envelope = narrowed_envelope();
    envelope.mandatory_guardrails.clear();
    assert_eq!(
        setup_violation_codes(envelope),
        vec!["weakened-mandatory-guardrail"]
    );
}

#[test]
fn a_setup_may_add_a_guardrail_but_not_drop_one() {
    let mut envelope = narrowed_envelope();
    envelope.mandatory_guardrails.insert(
        AgentGuardrailStageId::new("tenant-egress").expect("guardrail stage id should be valid"),
    );
    assert!(granted_envelope()
        .narrowing_violations(&envelope)
        .is_empty());
}

#[test]
fn a_setup_may_lower_a_budget_ceiling_but_never_raise_or_unbound_one() {
    let mut raised = narrowed_envelope();
    raised.budgets.max_model_calls = Some(50);
    assert_eq!(setup_violation_codes(raised), vec!["widened-budget"]);

    let mut unbounded = narrowed_envelope();
    unbounded.budgets.max_tokens = None;
    assert_eq!(setup_violation_codes(unbounded), vec!["widened-budget"]);

    // A dimension the definition left unbounded may be bounded by the setup:
    // that is a narrowing, not a widening.
    let mut bounded = narrowed_envelope();
    bounded.budgets.max_tool_calls = Some(2);
    assert!(granted_envelope().narrowing_violations(&bounded).is_empty());
}

#[test]
fn a_setup_may_not_add_a_coordination_capability_or_operation_class() {
    let mut envelope = narrowed_envelope();
    envelope
        .coordination_capabilities
        .insert(AgentCoordinationCapabilityKind::Delegation);
    envelope
        .operation_classes
        .insert(AgentOperationClass::Continuous);

    let codes = setup_violation_codes(envelope);
    assert!(
        codes.contains(&"unauthorized-coordination-capability"),
        "{codes:?}"
    );
    assert!(codes.contains(&"unadmitted-operation-class"), "{codes:?}");
}

#[test]
fn every_widening_is_reported_at_once() {
    // An operator fixing a rejected setup should see every problem, not just the
    // first one the checker happened to reach.
    let mut envelope = narrowed_envelope();
    envelope.tools.insert(
        tool("delete-account"),
        AgentToolDeclaration::new(AgentEffectSafetyClass::NonIdempotent),
    );
    envelope.model_profiles.insert(model("unreviewed-preview"));
    envelope.mandatory_guardrails.clear();

    let codes = setup_violation_codes(envelope);
    assert_eq!(codes.len(), 3, "{codes:?}");
    assert!(codes.contains(&"undeclared-tool"), "{codes:?}");
    assert!(codes.contains(&"unapproved-model-profile"), "{codes:?}");
    assert!(codes.contains(&"weakened-mandatory-guardrail"), "{codes:?}");
}

#[test]
fn a_definition_requires_a_bounded_outcome_oriented_description() {
    let id = rakka_agent::AgentDefinitionId::new("support-v1").expect("definition id");
    assert_eq!(
        AgentDefinition::new(id.clone(), "", AgentAuthorityEnvelope::empty())
            .expect_err("the description is mandatory")
            .code(),
        "empty-agent-description"
    );
    assert_eq!(
        AgentDefinition::new(
            id,
            "x".repeat(AGENT_DESCRIPTION_MAX_LENGTH + 1),
            AgentAuthorityEnvelope::empty()
        )
        .expect_err("the description is bounded")
        .code(),
        "agent-description-too-long"
    );
}

#[test]
fn a_definition_fails_closed_on_deserialization_when_its_description_is_out_of_bounds() {
    // The definition's fields are public, so construction alone cannot
    // guarantee the bounds. Deserialization is the wire and load path, and it
    // must reject what `AgentDefinition::new` rejects.
    let valid = AgentDefinition::new(
        rakka_agent::AgentDefinitionId::new("support-v1").expect("definition id"),
        "Resolves customer support tickets end to end.",
        AgentAuthorityEnvelope::empty(),
    )
    .expect("definition should be valid");

    let mut oversized = serde_json::to_value(&valid).expect("definition should serialize");
    oversized["description"] =
        serde_json::Value::String("x".repeat(AGENT_DESCRIPTION_MAX_LENGTH + 1));
    let error = serde_json::from_value::<AgentDefinition>(oversized)
        .expect_err("an oversized description must not cross the wire or load from a record");
    assert!(error.to_string().contains("exceeds"), "unexpected: {error}");

    let mut empty = serde_json::to_value(&valid).expect("definition should serialize");
    empty["description"] = serde_json::Value::String(String::new());
    let error = serde_json::from_value::<AgentDefinition>(empty)
        .expect_err("an empty description must not cross the wire or load from a record");
    assert!(
        error.to_string().contains("description"),
        "unexpected: {error}"
    );
}

#[test]
fn a_settings_revision_bounds_its_changes_on_deserialization() {
    let definition = definition();
    let initial = SettingsRevision::initial(&definition, AgentSettings::default(), provenance(2))
        .expect("initial settings should be accepted");
    let revision = initial
        .apply(
            &definition,
            vec![AgentSettingsChange::RetrievalLimit(4)],
            provenance(3),
        )
        .expect("the settings update should be accepted");

    let mut value = serde_json::to_value(&revision).expect("revision should serialize");
    let change = value["changes"][0].clone();
    value["changes"] = serde_json::Value::Array(vec![change; AGENT_SETTINGS_MAX_CHANGES + 1]);
    let error = serde_json::from_value::<SettingsRevision>(value)
        .expect_err("a change list beyond the bound must not cross the wire or load");
    assert!(error.to_string().contains("exceeds"), "unexpected: {error}");
}

#[test]
fn settings_changes_carry_their_application_timing() {
    assert_eq!(
        AgentSettingsChange::ModelProfile(model("mini")).timing_class(),
        SettingsTimingClass::TurnBound
    );
    assert_eq!(
        AgentSettingsChange::Sampling(AgentSamplingSettings::default()).timing_class(),
        SettingsTimingClass::TurnBound
    );
    assert_eq!(
        AgentSettingsChange::RevokeTool(tool("charge-card")).timing_class(),
        SettingsTimingClass::ImmediateSafety
    );
    assert_eq!(
        AgentSettingsChange::RevokeCredentialBinding(credential("stripe-live")).timing_class(),
        SettingsTimingClass::ImmediateSafety
    );
    assert_eq!(
        AgentSettingsChange::LoopStateSchemaVersion(StateSchemaVersion::new(2)).timing_class(),
        SettingsTimingClass::RunPinned
    );
}

#[test]
fn a_revision_applies_at_the_soonest_class_it_carries() {
    let definition = definition();
    let initial = SettingsRevision::initial(&definition, AgentSettings::default(), provenance(1))
        .expect("initial settings should be accepted");

    let mixed = initial
        .apply(
            &definition,
            vec![
                AgentSettingsChange::ModelProfile(model("mini")),
                AgentSettingsChange::RevokeTool(tool("charge-card")),
            ],
            provenance(2),
        )
        .expect("the update should be accepted");

    // A revision carrying both a prompt-class change and a revocation must be
    // honored before the next dispatch, not merely at the next turn.
    assert_eq!(
        mixed.application_point(),
        SettingsTimingClass::ImmediateSafety
    );
    assert!(mixed.has_immediate_safety_change());
    assert_eq!(mixed.revision(), AgentRevisionNumber::new(2));
}

#[test]
fn settings_cannot_select_a_model_the_definition_never_approved() {
    let definition = definition();
    let initial = SettingsRevision::initial(&definition, AgentSettings::default(), provenance(1))
        .expect("initial settings should be accepted");

    let error = initial
        .apply(
            &definition,
            vec![AgentSettingsChange::ModelProfile(model(
                "unreviewed-preview",
            ))],
            provenance(2),
        )
        .expect_err("settings must not widen the definition's model approvals");
    assert_eq!(error.code(), "envelope-widened");
    assert_eq!(error.violation_codes(), vec!["unapproved-model-profile"]);
}

#[test]
fn a_settings_update_is_bounded() {
    let definition = definition();
    let initial = SettingsRevision::initial(&definition, AgentSettings::default(), provenance(1))
        .expect("initial settings should be accepted");

    assert_eq!(
        initial
            .apply(&definition, Vec::new(), provenance(2))
            .expect_err("an empty update is not a revision")
            .code(),
        "empty-settings-update"
    );

    let changes = vec![AgentSettingsChange::RetrievalLimit(4); AGENT_SETTINGS_MAX_CHANGES + 1];
    assert_eq!(
        initial
            .apply(&definition, changes, provenance(2))
            .expect_err("durable state stays bounded")
            .code(),
        "too-many-settings-changes"
    );
}

#[test]
fn a_turn_reads_current_settings_but_keeps_its_pinned_schema() {
    let definition = definition();
    let pinned = SettingsRevision::initial(
        &definition,
        AgentSettings {
            model_profile: Some(model("frontier")),
            loop_state_schema_version: Some(StateSchemaVersion::new(1)),
            ..AgentSettings::default()
        },
        provenance(1),
    )
    .expect("initial settings should be accepted");

    let current = pinned
        .apply(
            &definition,
            vec![
                AgentSettingsChange::ModelProfile(model("mini")),
                AgentSettingsChange::RevokeTool(tool("charge-card")),
                AgentSettingsChange::LoopStateSchemaVersion(StateSchemaVersion::new(2)),
            ],
            provenance(2),
        )
        .expect("the update should be accepted");

    let effective = effective_settings_for_turn(&pinned, &current);

    // Turn-bound: the next turn sees the new model.
    assert_eq!(effective.model_profile, Some(model("mini")));
    // Immediate safety: the next dispatch sees the revocation.
    assert!(effective.revoked_tools.contains(&tool("charge-card")));
    // Run-pinned: the running run keeps the loop-state schema it started under.
    assert_eq!(
        effective.loop_state_schema_version,
        Some(StateSchemaVersion::new(1)),
        "a run-pinned schema change must not mutate a run already executing"
    );
    assert_eq!(
        current.settings().loop_state_schema_version,
        Some(StateSchemaVersion::new(2)),
        "the new run, however, starts under the new schema"
    );
}

#[test]
fn a_settings_revision_records_who_accepted_it() {
    let definition = definition();
    let initial = SettingsRevision::initial(&definition, AgentSettings::default(), provenance(1))
        .expect("initial settings should be accepted");
    let updated = initial
        .apply(
            &definition,
            vec![AgentSettingsChange::GuardrailPolicy(
                AgentPolicyRef::new("guardrails-v3").expect("policy ref should be valid"),
            )],
            provenance(9),
        )
        .expect("the update should be accepted");

    let provenance = updated.provenance();
    assert_eq!(provenance.principal.principal_id, "operator-1");
    assert_eq!(provenance.accepted_at, AgentTimestampMillis::new(9));
    assert_eq!(provenance.causation_id.as_str(), "cause-9");
    assert_eq!(provenance.audit_ref.as_str(), "audit-9");
}
