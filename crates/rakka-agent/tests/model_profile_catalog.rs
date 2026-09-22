//! The model profile record and catalog: a deployment-owned, secret-free
//! description of one approved model, resolved on demand.
//!
//! Spec section 4.1. The catalog returns a projection built on demand, so a
//! deployment whose profiles are release data implements it in one function
//! over its own record and stores nothing new; the static catalog is one
//! implementation among others. The digest is over the canonical
//! serialization, so two nodes projecting the same record agree.

use std::collections::BTreeMap;
use std::sync::Arc;

use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    AgentContextSnapshotRef, AgentCredentialBindingRef, AgentId, AgentModelCapabilities,
    AgentModelProfile, AgentModelProfileCatalog, AgentModelProfileId, AgentModelProviderKind,
    AgentModelRetryPolicy, AgentModelRouter, AgentModelTurn, AgentRevisionNumber, AgentRunId,
    AgentRunScope, AgentSamplingSettings, StaticAgentModelProfileCatalog, TenantId,
    AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES, AGENT_MODEL_PROFILE_MODEL_MAX_BYTES,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

fn profile_id(id: &str) -> AgentModelProfileId {
    AgentModelProfileId::new(id).expect("profile id should be valid")
}

fn binding(id: &str) -> AgentCredentialBindingRef {
    AgentCredentialBindingRef::new(id).expect("binding should be valid")
}

/// A context reference built the way every driver in this crate builds one:
/// derived from a run's scope and turn, never a bare literal
/// ([`AgentContextSnapshotRef::for_turn`]).
fn context_ref() -> AgentContextSnapshotRef {
    let scope = AgentRunScope::new(
        TenantId::new("acme"),
        AgentId::new("support-agent").expect("the agent id is valid"),
        AgentRunId::new("run-1").expect("the run id is valid"),
    )
    .expect("the scope is valid");
    AgentContextSnapshotRef::for_turn(&scope, 1).expect("the reference derives")
}

fn sonnet() -> AgentModelProfile {
    AgentModelProfile {
        profile_id: profile_id("anthropic-sonnet"),
        revision: AgentRevisionNumber::new(3),
        provider: AgentModelProviderKind::Anthropic,
        model: "claude-sonnet-5".to_string(),
        base_url: None,
        credential_binding: Some(binding("anthropic-key")),
        default_sampling: AgentSamplingSettings::default(),
        capabilities: AgentModelCapabilities { tool_calls: true },
        attributes: BTreeMap::from([("anthropic_version".to_string(), "2023-06-01".to_string())]),
    }
}

#[test]
fn a_valid_profile_validates_and_carries_no_secret_material() {
    sonnet().validate().expect("the profile is valid");
    let encoded = serde_json::to_string(&sonnet()).expect("encodes");
    assert!(
        !encoded.contains("sk-"),
        "a profile is a reference record: {encoded}"
    );
}

#[test]
fn a_base_url_with_userinfo_or_a_query_string_is_refused() {
    for url in [
        "https://user:pw@example.com/v1",
        "https://example.com/v1?key=abc",
        "not a url",
    ] {
        let mut profile = sonnet();
        profile.base_url = Some(url.to_string());
        let error = profile.validate().expect_err(url);
        assert_eq!(error.code(), "model-profile-invalid-base-url", "{url}");
    }
    let mut profile = sonnet();
    profile.base_url = Some("https://gateway.internal:8443/anthropic/v1".to_string());
    profile.validate().expect("a plain URL with a path is fine");
}

#[test]
fn secret_shaped_attribute_keys_and_oversized_values_are_refused() {
    for key in ["api_key", "token", "authorization", "secret", "API_KEY"] {
        let mut profile = sonnet();
        profile.attributes.insert(key.to_string(), "x".to_string());
        assert_eq!(
            profile.validate().expect_err(key).code(),
            "model-profile-invalid-attribute",
            "{key}"
        );
    }
    let mut profile = sonnet();
    profile.attributes.insert(
        "deployment".to_string(),
        "x".repeat(AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES + 1),
    );
    assert_eq!(
        profile.validate().expect_err("long").code(),
        "model-profile-invalid-attribute"
    );
    let mut profile = sonnet();
    profile.model = "m".repeat(AGENT_MODEL_PROFILE_MODEL_MAX_BYTES + 1);
    assert_eq!(
        profile.validate().expect_err("long model").code(),
        "model-profile-invalid-attribute"
    );
}

#[test]
fn the_digest_is_stable_across_two_projections_and_changes_with_content() {
    let a = sonnet();
    let b = sonnet();
    assert_eq!(
        a.digest(),
        b.digest(),
        "two nodes projecting one record agree"
    );
    let mut c = sonnet();
    c.model = "claude-opus-5".to_string();
    assert_ne!(a.digest(), c.digest());
    let mut d = sonnet();
    d.revision = AgentRevisionNumber::new(4);
    assert_ne!(a.digest(), d.digest(), "the revision is part of the record");
}

/// A deployment-owned record that is not `AgentModelProfile` implements the
/// catalog by projecting on demand; nothing is stored twice.
struct ReleaseProfile {
    id: &'static str,
    model: &'static str,
    release_revision: u64,
}

struct ReleaseCatalog {
    rows: Vec<ReleaseProfile>,
}

impl AgentModelProfileCatalog for ReleaseCatalog {
    fn profile(&self, id: &AgentModelProfileId) -> Option<AgentModelProfile> {
        let row = self.rows.iter().find(|row| row.id == id.as_str())?;
        Some(AgentModelProfile {
            profile_id: id.clone(),
            revision: AgentRevisionNumber::new(row.release_revision),
            provider: AgentModelProviderKind::Custom("openai-compatible".to_string()),
            model: row.model.to_string(),
            base_url: Some("https://llm.internal/v1".to_string()),
            credential_binding: None,
            default_sampling: AgentSamplingSettings::default(),
            capabilities: AgentModelCapabilities { tool_calls: true },
            attributes: BTreeMap::new(),
        })
    }
}

#[test]
fn a_deployment_owned_record_implements_the_catalog_without_a_second_store() {
    let catalog = ReleaseCatalog {
        rows: vec![ReleaseProfile {
            id: "release-default",
            model: "llama-3",
            release_revision: 12,
        }],
    };
    let projected = catalog
        .profile(&profile_id("release-default"))
        .expect("projected on demand");
    assert_eq!(projected.revision, AgentRevisionNumber::new(12));
    assert_eq!(
        projected.provider,
        AgentModelProviderKind::Custom("openai-compatible".to_string())
    );
    assert!(catalog.profile(&profile_id("missing")).is_none());
    assert_eq!(
        projected.digest(),
        catalog
            .profile(&profile_id("release-default"))
            .expect("again")
            .digest()
    );
}

#[test]
fn the_static_catalog_validates_on_insert_and_answers_by_id() {
    let catalog = StaticAgentModelProfileCatalog::new()
        .with_profile(sonnet())
        .expect("valid");
    assert!(catalog.profile(&profile_id("anthropic-sonnet")).is_some());
    assert!(catalog.profile(&profile_id("other")).is_none());
    let mut bad = sonnet();
    bad.base_url = Some("https://a:b@x/v1".to_string());
    assert_eq!(
        StaticAgentModelProfileCatalog::new()
            .with_profile(bad)
            .expect_err("refused")
            .code(),
        "model-profile-invalid-base-url"
    );
}

#[test]
fn the_router_selects_by_profile_and_refuses_an_unknown_one() {
    let sonnet_adapter = DeterministicModelAdapter::new()
        .with_turn(AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text("sonnet"));
    let default_adapter = DeterministicModelAdapter::new()
        .with_turn(AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text("default"));
    let router = AgentModelRouter::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_route(
            profile_id("anthropic-sonnet"),
            Arc::new(sonnet_adapter.clone()),
        )
        .expect("same adapter version")
        .with_default(Arc::new(default_adapter.clone()))
        .expect("same adapter version");

    let context = context_ref();
    let routed = rakka_agent::AgentModelRequest::new(context.clone(), 1)
        .with_profile(profile_id("anthropic-sonnet"));
    let unrouted = rakka_agent::AgentModelRequest::new(context.clone(), 1);
    let unknown =
        rakka_agent::AgentModelRequest::new(context, 1).with_profile(profile_id("nobody"));

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    use rakka_agent::AgentModelAdapter as _;
    let turn = rt.block_on(router.call(&routed)).expect("routed");
    assert_eq!(turn.text.as_deref(), Some("sonnet"));
    let turn = rt.block_on(router.call(&unrouted)).expect("default");
    assert_eq!(turn.text.as_deref(), Some("default"));
    let error = rt
        .block_on(router.call(&unknown))
        .expect_err("unknown profile");
    assert_eq!(error.code(), "model-profile-unknown");
    assert_eq!(sonnet_adapter.calls(), 1);
    assert_eq!(default_adapter.calls(), 1);
}

#[test]
fn the_router_refuses_a_route_whose_adapter_version_differs() {
    let other = DeterministicModelAdapter::new();
    let error = AgentModelRouter::new(AgentRevisionNumber::new(
        CURRENT_AGENT_LOOP_ADAPTER_VERSION.get() + 1,
    ))
    .with_route(profile_id("x"), Arc::new(other))
    .expect_err("a turn must carry one adapter version");
    assert_eq!(error.code(), "model-router-adapter-version-mismatch");
}

/// The router answers the retry policy its routes declare, because the
/// dispatcher enforces the adapter's declaration as the ceiling on every model
/// intent it dispatches: a router that answered the conservative default would
/// refuse every intent a routed adapter permits, and report a route's
/// non-idempotent declaration as read-only.
#[test]
fn the_router_answers_the_retry_policy_its_routes_declare() {
    use rakka_agent::AgentModelAdapter as _;

    let empty = AgentModelRouter::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION);
    assert_eq!(
        empty.retry_policy(),
        AgentModelRetryPolicy::DEFAULT,
        "a router with nothing routed declares the conservative default"
    );

    let policy = AgentModelRetryPolicy::read_only(3).expect("the policy is valid");
    let routed = DeterministicModelAdapter::new()
        .with_retry_policy(policy)
        .expect("the adapter policy is valid");
    let fallback = DeterministicModelAdapter::new()
        .with_retry_policy(policy)
        .expect("the adapter policy is valid");
    let router = AgentModelRouter::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_route(profile_id("anthropic-sonnet"), Arc::new(routed))
        .expect("the route agrees")
        .with_default(Arc::new(fallback))
        .expect("the default agrees");
    assert_eq!(router.retry_policy(), policy);
}

/// One retry policy per router, checked where an adapter enters exactly as the
/// adapter version is: the policy is a ceiling the dispatcher applies to the
/// intent *before* it knows which route will answer, so routes that disagree
/// have no single honest answer.
#[test]
fn the_router_refuses_a_route_whose_retry_policy_differs() {
    let strict = DeterministicModelAdapter::new();
    let lax = DeterministicModelAdapter::new()
        .with_retry_policy(AgentModelRetryPolicy::read_only(2).expect("the policy is valid"))
        .expect("the adapter policy is valid");

    let error = AgentModelRouter::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_route(profile_id("strict"), Arc::new(strict.clone()))
        .expect("the first route sets the policy")
        .with_route(profile_id("lax"), Arc::new(lax.clone()))
        .expect_err("a router declares one retry policy");
    assert_eq!(error.code(), "model-router-retry-policy-mismatch");

    // The default adapter is held to it too, in either order.
    let error = AgentModelRouter::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_default(Arc::new(lax))
        .expect("the default sets the policy")
        .with_route(profile_id("strict"), Arc::new(strict))
        .expect_err("a router declares one retry policy");
    assert_eq!(error.code(), "model-router-retry-policy-mismatch");
}
