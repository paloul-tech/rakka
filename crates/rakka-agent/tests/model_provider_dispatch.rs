//! The profile catalog at the authority, the credential path to the adapter,
//! and the deadline the resolver is handed.
//!
//! Spec section 4.2 (items 1 to 3) and 4.3. Every proof drives the real
//! dispatcher through `AuthorityFixture`, over the two halves of one path:
//!
//! - The authority half: the settings revision selects a profile, the catalog
//!   resolves the record, its binding is checked exactly as a tool's, and an
//!   unbounded credential-bearing call is refused before any attempt.
//! - The dispatcher half: the binding the grant carries is resolved inside the
//!   bounded attempt, the resolved credential reaches `call_with` and nothing
//!   else, the deadline the resolver reads is the attempt's own — stamped per
//!   attempt, absent from the durable record — and the request carries the
//!   model-visible tool list the authority derived.
//!
//! That the *credential material* reaches no durable surface is a different
//! claim with a different sweep: `secret_exclusion.rs` owns it, and carries
//! the profile-resolved scenario alongside the tool-resolved ones.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::*;
use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    AgentCredentialBindingRef, AgentEffectSpec, AgentModelCapabilities, AgentModelProfile,
    AgentModelProfileId, AgentModelProviderKind, AgentModelTurn, AgentRevisionNumber,
    AgentRunEffectRequest, AgentRunStatus, AgentSamplingSettings, AgentSettingsChange,
    AgentTaskContent, AgentToolAuthority, StaticAgentModelProfileCatalog,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentTimestampMillis;

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

// ---------------------------------------------------------------------------
// The dispatcher: the credential path, the deadline, and the tool list.
// ---------------------------------------------------------------------------

/// The model effect as durable state holds it once the run commits it, as
/// `(timeout_ms, deadline_at)`.
///
/// It reads *before* the first attempt on purpose: the deciding transition is
/// the only writer of the effect record the attempt then reads, so this is the
/// one moment at which "what the dispatcher was handed" is observable.
async fn committed_model_bound(
    fx: &AuthorityFixture,
) -> (Option<u64>, Option<AgentTimestampMillis>) {
    let state = rakka_agent::load_agent_run_state(
        &fx.fx.runs,
        &run_scope(),
        &rakka_agent::AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run exists");
    let effect = state
        .loop_state()
        .expect("the run has loop state")
        .effects()
        .iter()
        .find(|effect| matches!(effect.request, AgentRunEffectRequest::Model { .. }))
        .expect("the run committed a model effect")
        .clone();
    (effect.timeout_ms, effect.deadline_at)
}

/// The tool names the adapter's one model request was shown.
fn model_visible_tools(fx: &AuthorityFixture) -> Vec<String> {
    fx.adapter
        .requests()
        .into_iter()
        .next()
        .expect("one request")
        .tools
        .iter()
        .map(|tool| tool.tool.as_str().to_string())
        .collect()
}

/// The profile's binding is resolved inside the attempt through the existing
/// resolver and handed to `call_with`; the request carries the model-visible
/// tools; the resolver sees a deadline derived from the attempt bound, which
/// the durable record never keeps.
#[tokio::test]
async fn the_profile_credential_reaches_call_with_under_the_attempt_deadline() {
    let fx = profiled_fixture(true, Some(30_000));
    fx.start().await;
    select_profile(&fx).await;

    // The committed effect carries the spec's bound and no deadline: a durable
    // deadline would outlive the generation it bounds, so the stamp the
    // resolver reads below can only be the dispatcher's per-attempt clone.
    fx.settle().await;
    assert_eq!(committed_model_bound(&fx).await, (Some(30_000), None));

    fx.pump().await;

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(fx.adapter.calls(), 1);
    assert_eq!(
        fx.adapter.credentials_seen(),
        vec![Some("bearer-token")],
        "the adapter was handed the resolved credential, as its material kind"
    );
    let resolver = fx.credentials.as_ref().expect("resolver");
    assert_eq!(resolver.resolutions(), 1);
    let deadlines = resolver.deadlines();
    assert_eq!(deadlines.len(), 1);
    let deadline = deadlines[0].expect("a credential-bearing model call carries a deadline");
    // Pinned from both sides: the attempt started at or after the epoch of
    // this fixture's monotonic clock, and no later than the clock now reads,
    // so a deadline outside this window is not `start + timeout_ms`.
    assert!(
        (30_000..=fx.fx.now().as_millis() + 30_000).contains(&deadline.as_millis()),
        "the deadline is the attempt start plus the spec's timeout: {deadline:?}"
    );
    assert_eq!(
        model_visible_tools(&fx),
        vec![TOOL],
        "the request carries the model-visible tool list"
    );
}

/// A credential-free profile hands the adapter no credential at all: the
/// `call_with` path is taken either way, and what rides it is the grant's
/// binding, not the adapter's own configuration.
#[tokio::test]
async fn a_credential_free_profile_hands_the_adapter_no_credential() {
    let fx = profiled_fixture(false, None);
    fx.start().await;
    select_profile(&fx).await;
    fx.pump().await;

    assert_eq!(fx.adapter.calls(), 1);
    assert_eq!(fx.adapter.credentials_seen(), vec![None]);
    assert_eq!(
        fx.credentials.as_ref().expect("resolver").resolutions(),
        0,
        "no binding, no resolution"
    );
}

/// A tool the settings revoke is withheld from the request, not offered and
/// refused later.
#[tokio::test]
async fn a_revoked_tool_is_withheld_from_the_model_visible_list() {
    // The control, over the identical fixture: absent the revocation this run
    // shows the tool. Without it, "the list is empty" would also be true of a
    // dispatcher that never filled the list at all.
    let control = profiled_fixture(false, None);
    control.start().await;
    select_profile(&control).await;
    control.pump().await;
    assert_eq!(
        model_visible_tools(&control),
        vec![TOOL],
        "the unrevoked run is the control this test reads against"
    );

    let fx = profiled_fixture(false, None);
    fx.start().await;
    fx.apply_settings(
        "revoke-tool",
        vec![
            AgentSettingsChange::ModelProfile(profile_id()),
            AgentSettingsChange::RevokeTool(rakka_agent::AgentToolId::new(TOOL).expect("tool id")),
        ],
    )
    .await;
    fx.pump().await;
    assert!(
        model_visible_tools(&fx).is_empty(),
        "revoked tools never reach the model: {:?}",
        model_visible_tools(&fx)
    );
}

/// An adapter that does not override `call_with` still works: the default
/// forwards to `call`, so every existing adapter keeps compiling and running.
#[tokio::test]
async fn an_adapter_without_call_with_still_answers_through_call() {
    use rakka_agent::{AgentModelAdapter, AgentModelFuture, AgentModelRequest};

    struct CallOnly;
    impl AgentModelAdapter for CallOnly {
        fn adapter_version(&self) -> AgentRevisionNumber {
            CURRENT_AGENT_LOOP_ADAPTER_VERSION
        }
        fn call<'a>(&'a self, _request: &'a AgentModelRequest) -> AgentModelFuture<'a> {
            Box::pin(async { Ok(proposing_turn()) })
        }
    }
    let credential = rakka_agent_workflow::AgentEphemeralCredential::bearer_token("t");
    let context = rakka_agent::AgentContextSnapshotRef::for_turn(&run_scope(), 1)
        .expect("the snapshot reference derives");
    let request = AgentModelRequest::new(context, 1);
    let turn = CallOnly
        .call_with(&request, Some(&credential))
        .await
        .expect("forwards to call");
    assert_eq!(turn.text.as_deref(), Some("Done."));
}
