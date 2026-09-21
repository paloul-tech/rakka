//! The profile catalog at the authority, the credential path to the adapter,
//! and the deadline the resolver is handed.
//!
//! Spec section 4.2 (items 1 to 3) and 4.3. Every proof drives the real
//! dispatcher through `AuthorityFixture`: the authority resolves the profile
//! record, puts its binding on the grant, the dispatcher resolves it inside
//! the attempt and hands the credential to `call_with` under a per-attempt
//! deadline, and nothing about the credential is ever persisted.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::*;
use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    AgentCredentialBindingRef, AgentEffectSpec, AgentModelCapabilities, AgentModelProfile,
    AgentModelProfileId, AgentModelProviderKind, AgentModelTurn, AgentRevisionNumber,
    AgentRunStatus, AgentSamplingSettings, AgentSettingsChange, AgentTaskContent,
    AgentToolAuthority, StaticAgentModelProfileCatalog, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

const TOOL: &str = "charge-card";
const PROFILE: &str = "anthropic-sonnet";
const BINDING: &str = "anthropic-key";

fn profile_id() -> AgentModelProfileId {
    AgentModelProfileId::new(PROFILE).expect("profile id should be valid")
}

fn binding() -> AgentCredentialBindingRef {
    AgentCredentialBindingRef::new(BINDING).expect("binding should be valid")
}

fn profile(with_binding: bool) -> AgentModelProfile {
    AgentModelProfile {
        profile_id: profile_id(),
        revision: AgentRevisionNumber::new(3),
        provider: AgentModelProviderKind::Anthropic,
        model: "claude-sonnet-5".to_string(),
        base_url: None,
        credential_binding: with_binding.then(binding),
        default_sampling: AgentSamplingSettings::default(),
        capabilities: AgentModelCapabilities { tool_calls: true },
        attributes: BTreeMap::new(),
    }
}

fn catalog(with_binding: bool) -> Arc<StaticAgentModelProfileCatalog> {
    Arc::new(
        StaticAgentModelProfileCatalog::new()
            .with_profile(profile(with_binding))
            .expect("valid"),
    )
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A fixture whose definition approves the profile and its binding; `timeout`
/// is the model spec's per-attempt bound.
///
/// The agent's *current settings revision* selects the profile, which is what
/// `authorize_model` reads; the fixture applies settings through the entity's
/// own `UpdateSettings` command, so the selection is applied by
/// [`select_profile`] once the agent exists.
fn profiled_fixture(with_binding: bool, timeout_ms: Option<u64>) -> AuthorityFixture {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let mut envelope = envelope_for_registry(&registry);
    envelope.model_profiles.insert(profile_id());
    if with_binding {
        envelope.credential_bindings.insert(binding());
    }
    let mut spec = AgentEffectSpec::read_only();
    if let Some(timeout) = timeout_ms {
        spec = spec.with_timeout_ms(timeout);
    }
    AuthorityFixture::new(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn()),
        AgentToolAuthority::new(registry).with_model_profiles(catalog(with_binding)),
        Some(spec),
    )
    .with_envelope(envelope)
    .with_credential_resolver("sk-live-sentinel")
}

/// Moves the agent's current settings revision to one that selects `PROFILE`.
async fn select_profile(fx: &AuthorityFixture) {
    fx.apply_settings(
        "select-model-profile",
        vec![AgentSettingsChange::ModelProfile(profile_id())],
    )
    .await;
}

// ---------------------------------------------------------------------------
// The authority: profile resolution and the grant.
// ---------------------------------------------------------------------------

/// A settings revision that selects a profile the catalog does not know is
/// refused before any attempt, under `model-profile-unknown`.
#[tokio::test]
async fn an_unknown_profile_is_refused_at_the_authority() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let mut envelope = envelope_for_registry(&registry);
    envelope.model_profiles.insert(profile_id());
    let fx = AuthorityFixture::new(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn()),
        AgentToolAuthority::new(registry)
            .with_model_profiles(Arc::new(StaticAgentModelProfileCatalog::new())),
        None,
    )
    .with_envelope(envelope);
    fx.start().await;
    select_profile(&fx).await;
    fx.pump().await;
    assert_eq!(fx.terminal_failure_code().await, "model-profile-unknown");
    assert_eq!(
        fx.adapter.calls(),
        0,
        "no model is asked under an unknown profile"
    );
}

/// A profile whose binding the current settings revoke refuses the model
/// call exactly as a revoked tool binding does.
#[tokio::test]
async fn a_revoked_profile_binding_refuses_the_model_call() {
    let fx = profiled_fixture(true, Some(30_000));
    fx.start().await;
    select_profile(&fx).await;
    fx.apply_settings(
        "revoke-profile-binding",
        vec![AgentSettingsChange::RevokeCredentialBinding(binding())],
    )
    .await;
    fx.pump().await;
    assert_eq!(fx.terminal_failure_code().await, "credential-revoked");
    assert_eq!(fx.adapter.calls(), 0);
}

/// A credential-bearing model call whose spec carries no `timeout_ms` is
/// refused at the authority, before any resolver call.
#[tokio::test]
async fn a_credential_bearing_model_call_without_a_timeout_is_refused() {
    let fx = profiled_fixture(true, None);
    fx.start().await;
    select_profile(&fx).await;
    fx.pump().await;
    assert_eq!(fx.terminal_failure_code().await, "model-timeout-unset");
    assert_eq!(fx.adapter.calls(), 0);
    assert_eq!(
        fx.credentials.as_ref().expect("resolver").resolutions(),
        0,
        "the refusal precedes any resolver call"
    );
}

/// The same gate covers a binding the *intent* names. A deployment can put a
/// credential binding on the model effect spec with no profile binding at
/// all, and that secret reaches the dispatcher's resolver just the same, so
/// an unbounded attempt is refused for the identical reason.
#[tokio::test]
async fn an_intent_named_model_binding_without_a_timeout_is_refused() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let mut envelope = envelope_for_registry(&registry);
    envelope.model_profiles.insert(profile_id());
    envelope.credential_bindings.insert(binding());
    let fx = AuthorityFixture::new(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn()),
        AgentToolAuthority::new(registry).with_model_profiles(catalog(false)),
        // The model spec names the binding; the profile names none.
        Some(AgentEffectSpec::read_only().with_credential_binding(binding())),
    )
    .with_envelope(envelope)
    .with_credential_resolver("sk-live-sentinel");
    fx.start().await;
    select_profile(&fx).await;
    fx.pump().await;
    assert_eq!(fx.terminal_failure_code().await, "model-timeout-unset");
    assert_eq!(fx.adapter.calls(), 0);
    assert_eq!(
        fx.credentials.as_ref().expect("resolver").resolutions(),
        0,
        "the refusal precedes any resolver call"
    );
}

/// A profile without a binding needs no timeout and no resolver.
#[tokio::test]
async fn a_credential_free_profile_dispatches_without_a_timeout() {
    let fx = profiled_fixture(false, None);
    fx.start().await;
    select_profile(&fx).await;
    fx.pump().await;
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(fx.adapter.calls(), 1);
}
