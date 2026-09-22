# Phase 7 slice 7.1: wired model providers — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give a deployment a durable, secret-free model profile catalog, hand the adapter the credential the dispatcher resolves inside the attempt under the effect's own deadline, put the model-visible tool list on the request, add the provider telemetry slots, and ship an optional Rig provider adapter over an injected HTTP backend so every Rig 0.37 provider is reachable without a key ever leaving one attempt.

**Architecture:** Three seams first, adapter last. The authority resolves the profile record and puts its binding, revision, digest, and the model-visible tool list on the grant; the dispatcher resolves the credential through the existing resolver, stamps the attempt's deadline on the intent it hands out, and calls a new defaulted `call_with(request, credential)`; the turn and usage records gain optional provider fields that the OTel mapping exports under an allowlist. `RigProviderAdapter<H>` then builds a provider client per attempt over an injected `HttpClientExt` backend, delegates to the existing `RigModelAdapter`, and drops the client with the credential; `AgentModelRouter` selects an adapter by profile. `rig-core` gains only `rustls`.

**Tech Stack:** Rust 1.88 workspace (`rakka-agent`; `examples/durable-agent-acceptance`), `rig-core =0.37.0` (default features off, `rustls`), rig's `HttpClientExt` over `reqwest` 0.13, axum for the in-process fake provider, the `rakka-agent` testkit (`DeterministicModelAdapter`, `ScriptedCredentialResolver`, `AuthorityFixture`).

**Spec:** `docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md`, section 4 (all), 11.1, 11.2, 11.3, 12 (row 7.1). Section 4.2 item 3 (the credential path and deadline) and 4.5 (features) are binding as written; read them before Tasks 2, 3, and 5.

## Global Constraints

- Every public item needs a doc comment (`missing_docs = "warn"`; validation runs clippy with `-D warnings`). `unsafe_code = "forbid"`. MSRV 1.88.
- P2: every model call stays a durable outbox effect; this slice adds no second execution path. P3: a secret exists only as `AgentEphemeralCredential` inside one dispatch attempt; a profile carries a `credential_binding` reference and never material.
- `rig-core = { version = "=0.37.0", default-features = false, features = ["rustls"], optional = true }` and nothing else. The `reqwest` feature of `rig-core` (it enables `reqwest/system-proxy`) is never enabled by any Rakka crate, example, or test; a test parses the manifest and fails if it appears.
- `AgentModelAdapter::call_with` is **defaulted** to `self.call(request)` (spec decision 2): every in-tree and out-of-tree adapter keeps compiling; only the dispatcher calls it.
- `AgentDispatchAuthority`, `AgentRunEffectDispatcher::new`, `AgentToolAuthority::new`, `RigModelAdapter::new`, and every existing `with_*` builder keep their signatures. New behaviour arrives only through new builders and new optional fields.
- The credential handed to `call_with` is the one `AgentEffectCredentialResolver::resolve(scope, binding, effect)` produced inside the attempt; the effect handed to the resolver carries `timeout_ms` from `AgentEffectPolicies::spec_for` and, from this slice, a per-attempt `deadline_at = attempt start + timeout_ms` that is never persisted. A credential-bearing model call whose intent has no `timeout_ms` is refused at the authority with `model-timeout-unset`.
- `AgentModelRequest.tools` is the filtered model-visible set: `AgentToolRegistry::model_visible(envelope, settings)` keeps a descriptor only when the envelope declares the tool and the settings have not revoked it, narrowed further by the run's setup envelope when one exists.
- New serde-defaulted optional fields on versioned records (`AgentModelTurn`, `AgentModelUsage`, `AgentDispatchGrant`, `AgentGrantedDispatch`, `AgentModelRequest`) keep their schema version, the gap-slice-2 precedent: a record persisted before the field decodes with `None`.
- Stable codes this slice introduces, registered in Task 8: `model-profile-unknown`, `model-profile-invalid-base-url`, `model-profile-invalid-attribute`, `model-timeout-unset`, `model-credential-material-unsupported`, `model-router-adapter-version-mismatch`. `model-profile-revision-mismatch` is **not** answered in this slice (refinement 1 below).
- Test files: one concern per file under `crates/<crate>/tests/`; unit tests inline. Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Never push or open a PR without the owner's go-ahead (Task 9).
- On this machine run tests per crate and redirect `scripts/validate.sh` to a file; a run the harness moves to the background is read to completion before its exit code is reported.

## Refinements the tree forced (to be marked in the spec as "plan refinement 2026-09-21")

1. **Recovery mismatch is recorded, not enforced, in this slice.** The spec's 4.2 item 1 says a recovery that finds a materially different profile refuses `model-profile-revision-mismatch`. The only durable place a resumed attempt could compare against is the checkpoint grant, and `AgentCheckpointGrant` is built from `AgentCheckpointEffectBinding::of_effect(intent)` (`crates/rakka-agent/src/checkpoints.rs:299`), which sees the durable intent only; the intent carries a profile *id*, never the record's digest, and the run that parks it has no catalog. This slice records the profile's `revision` and `digest` on `AgentDispatchGrant` (audit, per attempt) and re-resolves the record on every attempt (unknown id and revoked binding refuse), and defers the cross-attempt refusal to a follow-up that carries the digest into the checkpoint binding. The spec sentence is amended in Task 8.
2. **The adapter refuses with stable codes through one new error variant.** `AgentModelError::Refused { code: &'static str, message: String }` (the enum is `#[non_exhaustive]`), so the provider adapter and the router answer `model-profile-invalid-base-url`, `model-credential-material-unsupported`, and `model-router-adapter-version-mismatch` without inventing per-case variants. `Basic` and `Custom` credential material are refused by the Rig adapter: rig's providers take an API key or a bearer token.
3. **Response model and finish reason come from the provider's raw response through an optional extractor.** `CompletionResponse<T>` carries `model` and `stop_reason`/`finish_reason` only inside `T` (rig 0.37, `completion/request.rs:352`), and a trait bound on `T` would break `RigModelAdapter` for providers Rakka does not name. `RigModelAdapter::with_response_metadata(fn(&M::Response) -> AgentModelResponseMetadata)` is an additive hook; the provider adapter installs it for the Anthropic and OpenAI-completions response types (whose fields are verified below), and cached and reasoning tokens come from rig's `Usage` for every provider.
4. **The tool list rides the grant.** The dispatcher holds no registry, so `AgentToolAuthority::authorize_model` computes `model_visible` and puts it on `AgentGrantedDispatch.tools`; the dispatcher copies it onto the request. Same derivation, same owner of the envelope and settings revision, as the spec asks.
5. **`AgentModelRouter` lives in the new `model_profile` module**, beside the catalog it keys on, not in `rig.rs`: it is provider-neutral.

## File structure

| File | Responsibility in this slice |
| --- | --- |
| `crates/rakka-agent/src/model_profile.rs` (new) | `AgentModelProviderKind`, `AgentModelCapabilities`, `AgentModelProfile` with `validate`/`digest`, `AgentModelProfileError`, `AgentModelProfileCatalog`, `StaticAgentModelProfileCatalog`, `AgentModelRouter` |
| `crates/rakka-agent/src/tools.rs` | `with_model_profiles`, the profile resolution in `authorize_model`, the timeout rule, the tool list and profile fields on the grant |
| `crates/rakka-agent/src/model.rs` | `AgentModelRequest.tools` + `with_tools`, `call_with`, `AgentModelError::Refused`, the turn and usage provider slots, `AgentModelResponseMetadata` |
| `crates/rakka-agent/src/dispatch.rs` | credential binding from the grant, per-attempt `deadline_at`, `call_with`, tools on the request, response metadata onto the attempt segment |
| `crates/rakka-agent/src/observability.rs`, `otel.rs` | segment slot for response metadata; four attribute keys; allowlist; mapping |
| `crates/rakka-agent/src/rig.rs` | tool definitions on the request, cached/reasoning tokens, the response-metadata hook, `RigProviderAdapter<H>` |
| `crates/rakka-agent/src/testkit.rs` | `DeterministicModelAdapter::call_with` recording credential kinds; `ScriptedCredentialResolver::deadlines()` |
| `crates/rakka-agent/Cargo.toml`, `src/lib.rs` | `rustls` feature, dev-dependencies for the fake endpoint, module and re-exports |
| `crates/rakka-agent/tests/model_profile_catalog.rs`, `model_provider_dispatch.rs`, `rig_provider_fake_endpoint.rs` (new); `secret_exclusion.rs`, `otel_span_mapping.rs`, `crate_shape.rs` (extended) | proofs |
| `examples/durable-agent-acceptance/**` | `World::with_model`, the env-backed resolver, `--provider`, the gated walk |
| `docs/**`, `CHANGELOG.md`, `CLAUDE.md` | Task 8 |

Verified at 3b44047 (the merge of PR #77): `AgentToolAuthority` fields `registry, guardrails, memory_ingress_attested, a2a_attested, execution_router, substrate_execution_policy, grant_ttl_ms`; `authorize_model` at `tools.rs:2203` builds `AgentGrantedDispatch { grant, tool_call: None, model_profile: profile, sampling: Some(settings.sampling), transforms, reports, checkpoint }`; `check_credential` at `:2854` checks the definition envelope, the setup envelope, and `settings.revoked_credential_bindings`; `AgentGrantedDispatch` at `:998`, `AgentDispatchGrant` at `:905` (has `credential_binding`), `AgentGrantDescriptor` at `:883`; the dispatcher's `attempt_invocation` at `dispatch.rs:2465` resolves the credential at `:2678–2745` from `intent.credential_binding` and calls `self.invoke(scope, intent, &granted, credential.as_ref())` at `:2752`; the Model arm at `:3519–3549` calls `self.model.call(&request)` at `:3537`; `attempt_segment.usage(turn.usage)` at `:2802`; `AgentModelAdapter` at `model.rs:604`; `AgentModelRequest` at `:380` with `new(context, turn)`, `with_settings_revision`, `with_sampling`, `with_profile`, `with_telemetry`; `AgentModelUsage { input_tokens, output_tokens, cost_micros }` at `:105` (`Copy + Default`); the turn's shadow record `AgentModelTurnRecord` at `:330`; `AgentModelError` at `:626` with `code()` at `:694`; `AgentEffectPolicies::new()` gives the model spec `AgentEffectSpec::read_only()` (`effect.rs:638`); `AgentRunEffect` is `Clone` (`effect.rs:1774`); `AgentEffectCredentialResolver` at `dispatch.rs:1346`; `AgentModelProfileId` is a `validated_id!` (`definition.rs:104`); `ScriptedCredentialResolver` at `testkit.rs:3402`; `DeterministicModelAdapter` at `:1856` with `requests()`; rig 0.37: `ClientBuilder::http_client` (`client/mod.rs:658`), `build` for `H: HttpClientExt` (`:711–718`), `impl HttpClientExt for reqwest::Client` unconditional (`http_client/mod.rs:137`), `Client::builder()` on `impl<Ext> Client<Ext, reqwest::Client>` (`:336`), OpenAI `completions_api()` requires `H: HttpClientExt + Clone + Debug + Default + Send + Sync + 'static` (`providers/openai/client.rs:124–131`), Anthropic `ClientBuilder<H>::anthropic_version` (`providers/anthropic/client.rs:145`), Azure `api_version`/`azure_endpoint` (`azure.rs:167,176`) with `AzureOpenAIAuth::{ApiKey, Token}` (`:187`), `OllamaApiKey: From<String>` (`ollama.rs:88`), `GeminiApiKey: From<S: Into<String>>` (`gemini/client.rs:41`), `AnthropicKey: From<S: Into<String>>` (`anthropic/client.rs:47`); rig `Usage { input_tokens, output_tokens, total_tokens, cached_input_tokens, cache_creation_input_tokens, reasoning_tokens }` (`completion/request.rs:395`); Anthropic `CompletionResponse { model: String, stop_reason: Option<String>, .. }` (`providers/anthropic/completion.rs:56`); OpenAI completions `CompletionResponse { model: String, choices: Vec<Choice { finish_reason: String, .. }>, .. }` (`providers/openai/completion/mod.rs:898, 1062`).

---

### Task 1: The model profile record, catalog trait, static catalog, and router

**Files:**
- Create: `crates/rakka-agent/src/model_profile.rs`
- Modify: `crates/rakka-agent/src/lib.rs` (add `pub mod model_profile;` in the module list, alphabetically after `pub mod model;`, and a `pub use model_profile::{…}` block)
- Modify: `crates/rakka-agent/src/model.rs` (add `AgentModelError::Refused`)
- Create: `crates/rakka-agent/tests/model_profile_catalog.rs`

**Interfaces:**
- Consumes: `AgentModelProfileId` (`crate::definition`), `AgentCredentialBindingRef`, `AgentRevisionNumber`, `AgentSamplingSettings` (`crate::model`), `AgentContentDigest::of_json` (`crate::task`), `AgentModelAdapter`, `AgentModelRequest`, `AgentModelFuture`, `AgentModelRetryPolicy`, `AgentEphemeralCredential` (`rakka_agent_workflow`).
- Produces:
  - `pub enum AgentModelProviderKind { Anthropic, OpenAiResponses, OpenAiCompletions, AzureOpenAi, Gemini, Ollama, OpenRouter, Custom(String) }` (`#[non_exhaustive]`, `Serialize`, `Deserialize`, kebab-case, `as_label() -> Cow<'_, str>` hmm — `as_label(&self) -> String` for `Custom`), `pub struct AgentModelCapabilities { pub tool_calls: bool }`.
  - `pub struct AgentModelProfile { pub profile_id, pub revision, pub provider, pub model: String, pub base_url: Option<String>, pub credential_binding: Option<AgentCredentialBindingRef>, pub default_sampling: AgentSamplingSettings, pub capabilities: AgentModelCapabilities, pub attributes: BTreeMap<String, String> }` with `validate(&self) -> Result<(), AgentModelProfileError>` and `digest(&self) -> AgentContentDigest`.
  - `pub enum AgentModelProfileError { ModelNameTooLong { bytes }, InvalidBaseUrl { reason: &'static str }, ForbiddenAttribute { key: String }, AttributeTooLong { key: String, bytes } }` with `code()`: `model-profile-invalid-attribute` for the model-name and attribute cases, `model-profile-invalid-base-url` for the URL.
  - `pub trait AgentModelProfileCatalog: Send + Sync + 'static { fn profile(&self, id: &AgentModelProfileId) -> Option<AgentModelProfile>; }`, `pub struct StaticAgentModelProfileCatalog` with `new()`, `with_profile(self, profile) -> Result<Self, AgentModelProfileError>`.
  - `pub struct AgentModelRouter` with `new(adapter_version: AgentRevisionNumber)`, `with_route(self, id: AgentModelProfileId, adapter: Arc<dyn AgentModelAdapter>) -> Result<Self, AgentModelError>`, `with_default(self, adapter) -> Result<Self, AgentModelError>`, implementing `AgentModelAdapter` (`call` and `call_with` both route on `request.profile`).
  - Constants: `AGENT_MODEL_PROFILE_MODEL_MAX_BYTES = 128`, `AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES = 256`, `AGENT_MODEL_PROFILE_FORBIDDEN_ATTRIBUTE_KEYS: [&str; 4] = ["api_key", "token", "authorization", "secret"]`.
  - `AgentModelError::Refused { code: &'static str, message: String }` whose `code()` answers `code`.

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-agent/tests/model_profile_catalog.rs`:

```rust
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

use rakka_agent::{
    AgentCredentialBindingRef, AgentModelCapabilities, AgentModelProfile,
    AgentModelProfileCatalog, AgentModelProfileId, AgentModelProviderKind, AgentModelRouter,
    AgentModelTurn, AgentRevisionNumber, AgentSamplingSettings, StaticAgentModelProfileCatalog,
    AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES, AGENT_MODEL_PROFILE_MODEL_MAX_BYTES,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent::testkit::DeterministicModelAdapter;

fn profile_id(id: &str) -> AgentModelProfileId {
    AgentModelProfileId::new(id).expect("profile id should be valid")
}

fn binding(id: &str) -> AgentCredentialBindingRef {
    AgentCredentialBindingRef::new(id).expect("binding should be valid")
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
    assert!(!encoded.contains("sk-"), "a profile is a reference record: {encoded}");
}

#[test]
fn a_base_url_with_userinfo_or_a_query_string_is_refused() {
    for url in ["https://user:pw@example.com/v1", "https://example.com/v1?key=abc", "not a url"] {
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
        assert_eq!(profile.validate().expect_err(key).code(), "model-profile-invalid-attribute", "{key}");
    }
    let mut profile = sonnet();
    profile.attributes.insert("deployment".to_string(), "x".repeat(AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES + 1));
    assert_eq!(profile.validate().expect_err("long").code(), "model-profile-invalid-attribute");
    let mut profile = sonnet();
    profile.model = "m".repeat(AGENT_MODEL_PROFILE_MODEL_MAX_BYTES + 1);
    assert_eq!(profile.validate().expect_err("long model").code(), "model-profile-invalid-attribute");
}

#[test]
fn the_digest_is_stable_across_two_projections_and_changes_with_content() {
    let a = sonnet();
    let b = sonnet();
    assert_eq!(a.digest(), b.digest(), "two nodes projecting one record agree");
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
        rows: vec![ReleaseProfile { id: "release-default", model: "llama-3", release_revision: 12 }],
    };
    let projected = catalog.profile(&profile_id("release-default")).expect("projected on demand");
    assert_eq!(projected.revision, AgentRevisionNumber::new(12));
    assert_eq!(projected.provider, AgentModelProviderKind::Custom("openai-compatible".to_string()));
    assert!(catalog.profile(&profile_id("missing")).is_none());
    assert_eq!(projected.digest(), catalog.profile(&profile_id("release-default")).expect("again").digest());
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
        StaticAgentModelProfileCatalog::new().with_profile(bad).expect_err("refused").code(),
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
        .with_route(profile_id("anthropic-sonnet"), Arc::new(sonnet_adapter.clone()))
        .expect("same adapter version")
        .with_default(Arc::new(default_adapter.clone()))
        .expect("same adapter version");

    let context = rakka_agent::AgentContextSnapshotRef::new("snapshot-1").expect("ref");
    let routed = rakka_agent::AgentModelRequest::new(context.clone(), 1).with_profile(profile_id("anthropic-sonnet"));
    let unrouted = rakka_agent::AgentModelRequest::new(context.clone(), 1);
    let unknown = rakka_agent::AgentModelRequest::new(context, 1).with_profile(profile_id("nobody"));

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    use rakka_agent::AgentModelAdapter as _;
    let turn = rt.block_on(router.call(&routed)).expect("routed");
    assert_eq!(turn.text.as_deref(), Some("sonnet"));
    let turn = rt.block_on(router.call(&unrouted)).expect("default");
    assert_eq!(turn.text.as_deref(), Some("default"));
    let error = rt.block_on(router.call(&unknown)).expect_err("unknown profile");
    assert_eq!(error.code(), "model-profile-unknown");
    assert_eq!(sonnet_adapter.calls(), 1);
    assert_eq!(default_adapter.calls(), 1);
}

#[test]
fn the_router_refuses_a_route_whose_adapter_version_differs() {
    let other = DeterministicModelAdapter::new();
    let error = AgentModelRouter::new(AgentRevisionNumber::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION.get() + 1))
        .with_route(profile_id("x"), Arc::new(other))
        .expect_err("a turn must carry one adapter version");
    assert_eq!(error.code(), "model-router-adapter-version-mismatch");
}
```

If `AgentContextSnapshotRef::new` has a different constructor, use the one `model_adapter.rs` in the same tests directory uses to build a request. If `AgentCredentialBindingRef::new` is not the constructor, follow `tests/common/mod.rs`. `tokio` is available to tests through the crate's dependencies; if `tokio::runtime::Runtime` is not reachable, mark the two router tests `#[tokio::test] async` instead.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p rakka-agent --test model_profile_catalog`
Expected: compile error; the module and types do not exist.

- [ ] **Step 3: Add `AgentModelError::Refused`**

In `crates/rakka-agent/src/model.rs`, add to `AgentModelError`:

```rust
    /// The adapter refused the call under a stable code of its own, before
    /// or instead of asking a provider — an invalid profile, unsupported
    /// credential material, or a route that does not exist.
    Refused {
        /// The stable code the refusal is answered under.
        code: &'static str,
        /// Bounded human-readable detail.
        message: String,
    },
```

with `Self::Refused { code, .. } => code` in `code()` and `Self::Refused { code, message } => write!(f, "the model adapter refused the call ({code}): {message}")` in `Display`. If `AgentModelError` also has an `is_retryable`/failure-class method, a `Refused` is definitive (not retryable); follow the `Provider` arm's shape for any other match.

- [ ] **Step 4: Write the module**

Create `crates/rakka-agent/src/model_profile.rs`:

```rust
//! The model profile record and catalog
//! ([specification 10.1](../../../docs/plans/rakka-agent/spec.md); Phase 7
//! design section 4.1).
//!
//! A profile is a deployment-owned, secret-free description of one approved
//! model: which provider, which model name, an optional non-default endpoint,
//! the logical credential binding a dispatch attempt may resolve, default
//! sampling, and bounded non-secret attributes. [`AgentModelProfileId`] stays
//! the opaque id the definition envelope approves; the record behind it is
//! resolved on demand through [`AgentModelProfileCatalog`], so a deployment
//! whose profiles are release data implements the trait in one function over
//! its own record and stores nothing new. [`StaticAgentModelProfileCatalog`]
//! is one implementation, for tests, examples, and deployments with no
//! release data.
//!
//! [`AgentModelRouter`] is the provider-neutral half of the adapter story: an
//! [`AgentModelAdapter`] that selects one of several adapters by the
//! request's profile.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::definition::{AgentCredentialBindingRef, AgentModelProfileId, AgentRevisionNumber};
use crate::model::{
    AgentModelAdapter, AgentModelError, AgentModelFuture, AgentModelRequest, AgentModelRetryPolicy,
    AgentSamplingSettings,
};
use crate::task::AgentContentDigest;
use rakka_agent_workflow::AgentEphemeralCredential;

/// Longest provider model name a profile may carry, in bytes.
pub const AGENT_MODEL_PROFILE_MODEL_MAX_BYTES: usize = 128;

/// Longest attribute value a profile may carry, in bytes.
pub const AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES: usize = 256;

/// Attribute keys a profile may never carry, compared case-insensitively:
/// a value under one of these is secret-shaped whatever it holds.
pub const AGENT_MODEL_PROFILE_FORBIDDEN_ATTRIBUTE_KEYS: [&str; 4] =
    ["api_key", "token", "authorization", "secret"];

/// The provider family a profile addresses.
///
/// Rig does not gate providers individually, so every kind is reachable
/// once the `rig` feature is on; `Custom` names an OpenAI-compatible
/// endpoint by a deployment's own label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentModelProviderKind {
    /// Anthropic's Messages API.
    Anthropic,
    /// OpenAI's Responses API.
    OpenAiResponses,
    /// OpenAI's Chat Completions API.
    OpenAiCompletions,
    /// Azure OpenAI.
    AzureOpenAi,
    /// Google Gemini's generateContent API.
    Gemini,
    /// A local or remote Ollama server.
    Ollama,
    /// OpenRouter.
    OpenRouter,
    /// An OpenAI-compatible endpoint under a deployment-chosen label.
    Custom(String),
}

impl AgentModelProviderKind {
    /// Stable kebab-case label; `Custom` answers its own label.
    #[must_use]
    pub fn as_label(&self) -> String {
        match self {
            Self::Anthropic => "anthropic".to_string(),
            Self::OpenAiResponses => "openai-responses".to_string(),
            Self::OpenAiCompletions => "openai-completions".to_string(),
            Self::AzureOpenAi => "azure-openai".to_string(),
            Self::Gemini => "gemini".to_string(),
            Self::Ollama => "ollama".to_string(),
            Self::OpenRouter => "openrouter".to_string(),
            Self::Custom(label) => label.clone(),
        }
    }
}

/// What a profile's model can do, as the deployment declares it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AgentModelCapabilities {
    /// Whether the model accepts tool definitions and may answer with tool calls.
    pub tool_calls: bool,
}

/// One approved model, described without any secret.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentModelProfile {
    /// The id the definition envelope approves.
    pub profile_id: AgentModelProfileId,
    /// The record's revision, part of its digest.
    pub revision: AgentRevisionNumber,
    /// The provider family.
    pub provider: AgentModelProviderKind,
    /// The provider's model name, at most [`AGENT_MODEL_PROFILE_MODEL_MAX_BYTES`].
    pub model: String,
    /// A non-default endpoint; no userinfo, no query string.
    pub base_url: Option<String>,
    /// The logical credential binding a dispatch attempt resolves; the only
    /// way a profile reaches a secret.
    pub credential_binding: Option<AgentCredentialBindingRef>,
    /// Sampling defaults a settings revision may narrow.
    pub default_sampling: AgentSamplingSettings,
    /// Declared capabilities.
    pub capabilities: AgentModelCapabilities,
    /// Bounded, non-secret provider attributes (an API version, a deployment name).
    pub attributes: BTreeMap<String, String>,
}

/// Why a profile record is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentModelProfileError {
    /// The model name exceeds its bound.
    ModelNameTooLong {
        /// Length in bytes.
        bytes: usize,
    },
    /// The base URL is not a plain URL without userinfo or a query string.
    InvalidBaseUrl {
        /// The refused property.
        reason: &'static str,
    },
    /// An attribute key is secret-shaped.
    ForbiddenAttribute {
        /// The key.
        key: String,
    },
    /// An attribute value exceeds its bound.
    AttributeTooLong {
        /// The key.
        key: String,
        /// Length in bytes.
        bytes: usize,
    },
}

impl AgentModelProfileError {
    /// Stable code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidBaseUrl { .. } => "model-profile-invalid-base-url",
            Self::ModelNameTooLong { .. }
            | Self::ForbiddenAttribute { .. }
            | Self::AttributeTooLong { .. } => "model-profile-invalid-attribute",
        }
    }
}

impl Display for AgentModelProfileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ModelNameTooLong { bytes } => write!(
                f,
                "the model name is {bytes} bytes; at most {AGENT_MODEL_PROFILE_MODEL_MAX_BYTES} are allowed"
            ),
            Self::InvalidBaseUrl { reason } => write!(f, "the base URL is refused: {reason}"),
            Self::ForbiddenAttribute { key } => {
                write!(f, "the attribute key {key} is secret-shaped and never allowed on a profile")
            }
            Self::AttributeTooLong { key, bytes } => write!(
                f,
                "the attribute {key} is {bytes} bytes; at most {AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES} are allowed"
            ),
        }
    }
}

impl std::error::Error for AgentModelProfileError {}

/// Splits `scheme://authority/rest` and refuses anything that is not that shape.
fn check_base_url(url: &str) -> Result<(), AgentModelProfileError> {
    let Some((scheme, rest)) = url.split_once("://") else {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "it does not parse as scheme://host",
        });
    };
    if !matches!(scheme, "http" | "https") {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "the scheme is not http or https",
        });
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.is_empty() {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "the host is empty",
        });
    }
    if authority.contains('@') {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "it carries userinfo",
        });
    }
    if rest.contains('?') || rest.contains('#') {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "it carries a query string or fragment",
        });
    }
    if url.chars().any(char::is_whitespace) {
        return Err(AgentModelProfileError::InvalidBaseUrl {
            reason: "it carries whitespace",
        });
    }
    Ok(())
}

impl AgentModelProfile {
    /// Refuses a record that could carry a secret or exceed a bound.
    ///
    /// # Errors
    ///
    /// [`AgentModelProfileError`] with its stable code.
    pub fn validate(&self) -> Result<(), AgentModelProfileError> {
        if self.model.is_empty() || self.model.len() > AGENT_MODEL_PROFILE_MODEL_MAX_BYTES {
            return Err(AgentModelProfileError::ModelNameTooLong {
                bytes: self.model.len(),
            });
        }
        if let Some(url) = &self.base_url {
            check_base_url(url)?;
        }
        for (key, value) in &self.attributes {
            let folded = key.to_ascii_lowercase();
            if AGENT_MODEL_PROFILE_FORBIDDEN_ATTRIBUTE_KEYS
                .iter()
                .any(|forbidden| folded == *forbidden || folded.contains(forbidden))
            {
                return Err(AgentModelProfileError::ForbiddenAttribute { key: key.clone() });
            }
            if value.len() > AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES {
                return Err(AgentModelProfileError::AttributeTooLong {
                    key: key.clone(),
                    bytes: value.len(),
                });
            }
        }
        Ok(())
    }

    /// A digest over the record's canonical serialization: what a dispatch
    /// grant records, and what two nodes projecting one record agree on.
    #[must_use]
    pub fn digest(&self) -> AgentContentDigest {
        let value = serde_json::to_value(self).unwrap_or(serde_json::Value::Null);
        AgentContentDigest::of_json(&value)
    }
}

/// Resolves a profile record on demand.
pub trait AgentModelProfileCatalog: Send + Sync + 'static {
    /// The record behind an approved id, or `None` when the deployment knows no such profile.
    fn profile(&self, id: &AgentModelProfileId) -> Option<AgentModelProfile>;
}

/// An in-memory catalog validated on insert; one implementation among others.
#[derive(Debug, Clone, Default)]
pub struct StaticAgentModelProfileCatalog {
    profiles: BTreeMap<AgentModelProfileId, AgentModelProfile>,
}

impl StaticAgentModelProfileCatalog {
    /// An empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a profile after validating it.
    ///
    /// # Errors
    ///
    /// The profile's own refusal.
    pub fn with_profile(mut self, profile: AgentModelProfile) -> Result<Self, AgentModelProfileError> {
        profile.validate()?;
        self.profiles.insert(profile.profile_id.clone(), profile);
        Ok(self)
    }
}

impl AgentModelProfileCatalog for StaticAgentModelProfileCatalog {
    fn profile(&self, id: &AgentModelProfileId) -> Option<AgentModelProfile> {
        self.profiles.get(id).cloned()
    }
}

/// An adapter that selects one of several adapters by the request's profile.
///
/// Every route must stamp the same adapter version as the router declares,
/// because the version is persisted with each turn and an upgrade is an
/// explicit migration ([specification 10.2](../../../docs/plans/rakka-agent/spec.md)):
/// a router mixing versions would write turns no single migration describes.
pub struct AgentModelRouter {
    adapter_version: AgentRevisionNumber,
    routes: BTreeMap<AgentModelProfileId, Arc<dyn AgentModelAdapter>>,
    default: Option<Arc<dyn AgentModelAdapter>>,
}

impl fmt::Debug for AgentModelRouter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentModelRouter")
            .field("adapter_version", &self.adapter_version)
            .field("routes", &self.routes.keys().collect::<Vec<_>>())
            .field("default", &self.default.is_some())
            .finish()
    }
}

impl AgentModelRouter {
    /// A router with no routes, declaring the adapter version every route must share.
    #[must_use]
    pub fn new(adapter_version: AgentRevisionNumber) -> Self {
        Self {
            adapter_version,
            routes: BTreeMap::new(),
            default: None,
        }
    }

    fn check_version(&self, adapter: &Arc<dyn AgentModelAdapter>) -> Result<(), AgentModelError> {
        let version = adapter.adapter_version();
        if version != self.adapter_version {
            return Err(AgentModelError::Refused {
                code: "model-router-adapter-version-mismatch",
                message: format!(
                    "the router stamps adapter version {} and the route stamps {version}",
                    self.adapter_version
                ),
            });
        }
        Ok(())
    }

    /// Routes one profile to an adapter.
    ///
    /// # Errors
    ///
    /// `model-router-adapter-version-mismatch` when the adapter's version differs.
    pub fn with_route(
        mut self,
        profile: AgentModelProfileId,
        adapter: Arc<dyn AgentModelAdapter>,
    ) -> Result<Self, AgentModelError> {
        self.check_version(&adapter)?;
        self.routes.insert(profile, adapter);
        Ok(self)
    }

    /// The adapter an unprofiled request goes to.
    ///
    /// # Errors
    ///
    /// `model-router-adapter-version-mismatch` when the adapter's version differs.
    pub fn with_default(mut self, adapter: Arc<dyn AgentModelAdapter>) -> Result<Self, AgentModelError> {
        self.check_version(&adapter)?;
        self.default = Some(adapter);
        Ok(self)
    }

    fn route(&self, request: &AgentModelRequest) -> Result<&Arc<dyn AgentModelAdapter>, AgentModelError> {
        match &request.profile {
            Some(profile) => self.routes.get(profile).ok_or_else(|| AgentModelError::Refused {
                code: "model-profile-unknown",
                message: format!("no adapter is routed for the model profile {profile}"),
            }),
            None => self.default.as_ref().ok_or_else(|| AgentModelError::Refused {
                code: "model-profile-unknown",
                message: "the request names no profile and the router has no default adapter".to_string(),
            }),
        }
    }
}

impl AgentModelAdapter for AgentModelRouter {
    fn adapter_version(&self) -> AgentRevisionNumber {
        self.adapter_version
    }

    fn retry_policy(&self) -> AgentModelRetryPolicy {
        AgentModelRetryPolicy::DEFAULT
    }

    fn call<'a>(&'a self, request: &'a AgentModelRequest) -> AgentModelFuture<'a> {
        Box::pin(async move { self.route(request)?.call(request).await })
    }

    fn call_with<'a>(
        &'a self,
        request: &'a AgentModelRequest,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentModelFuture<'a> {
        Box::pin(async move { self.route(request)?.call_with(request, credential).await })
    }
}
```

`call_with` on the trait is added in Task 3; until then, in this task implement only `call` on the router and add the `call_with` forwarding in Task 3 (Task 3 names it). If `AgentModelProfileId` does not implement `Display`, use `profile.as_str()` in the message. `AgentContentDigest: Serialize`? It is persisted on grants, so yes.

Declare the module and re-exports in `lib.rs`:

```rust
pub mod model_profile;
…
pub use model_profile::{
    AgentModelCapabilities, AgentModelProfile, AgentModelProfileCatalog, AgentModelProfileError,
    AgentModelProviderKind, AgentModelRouter, StaticAgentModelProfileCatalog,
    AGENT_MODEL_PROFILE_ATTRIBUTE_MAX_BYTES, AGENT_MODEL_PROFILE_FORBIDDEN_ATTRIBUTE_KEYS,
    AGENT_MODEL_PROFILE_MODEL_MAX_BYTES,
};
```

and add a line for `model_profile` to the crate's module map doc comment in `lib.rs` if that comment enumerates modules (follow the neighbouring lines' shape).

- [ ] **Step 5: Run the tests**

Run: `cargo test -p rakka-agent --test model_profile_catalog` then `cargo test -p rakka-agent --test crate_shape` (the module-map test) then `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings` and `cargo fmt --all -- --check`.
Expected: all pass. The two router tests that call `call_with` are added in Task 3; in this task the router test uses `call` only, exactly as written.

- [ ] **Step 6: Commit**

```bash
git checkout -b rakka-agents-phase7-slice-7-1 rakka-agents
git add crates/rakka-agent/src/model_profile.rs crates/rakka-agent/src/model.rs crates/rakka-agent/src/lib.rs crates/rakka-agent/tests/model_profile_catalog.rs
git commit -m "Describe an approved model as a secret-free profile record resolved on demand, and route adapters by profile

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: The authority resolves the profile, puts its binding, revision, digest, and the model-visible tool list on the grant, and refuses a credential-bearing call with no timeout

**Files:**
- Modify: `crates/rakka-agent/src/tools.rs` (the struct at ~1385 and `new` at 1407; a new builder after `with_a2a_guardrails` at 1528; `authorize_model` at 2203; `AgentGrantedDispatch` at 998; `AgentDispatchGrant` at 905; `grant()` near 2700)
- Modify: `crates/rakka-agent/src/lib.rs` (no new names; the constant list is unchanged)
- Create: `crates/rakka-agent/tests/model_provider_dispatch.rs` (authority-level half)

**Interfaces:**
- Consumes: Task 1's `AgentModelProfileCatalog`, `AgentModelProfile::digest`; `AgentToolRegistry::model_visible(envelope, settings) -> Vec<&AgentToolDescriptor>` (`tools.rs:723`); `check_credential(context, &binding)`; `AuthorityFixture` (`tests/common/mod.rs`) with `with_envelope`, `with_credential_resolver`, `terminal_failure_code`.
- Produces:
  - `AgentToolAuthority::with_model_profiles(self, catalog: Arc<dyn AgentModelProfileCatalog>) -> Self`.
  - `AgentGrantedDispatch` gains `pub model_credential_binding: Option<AgentCredentialBindingRef>` and `pub tools: Vec<AgentToolDescriptor>` (both `#[serde(default)]` if the type is serialized; it is not persisted, so plain fields).
  - `AgentDispatchGrant` gains `#[serde(default)] pub model_profile_revision: Option<AgentRevisionNumber>` and `#[serde(default)] pub model_profile_digest: Option<AgentContentDigest>`; `credential_binding` is set from the profile's binding when the intent carries none.
  - `authorize_model` refuses `model-profile-unknown` (catalog installed, id unknown), the profile's revoked or unapproved binding through `check_credential`, and `model-timeout-unset` (binding present, `intent.timeout_ms == None`).

- [ ] **Step 1: Write the failing end-to-end tests**

Create `crates/rakka-agent/tests/model_provider_dispatch.rs`:

```rust
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
    AgentRunStatus, AgentSamplingSettings, AgentSettings, AgentSettingsChange, AgentTaskContent,
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

/// A fixture whose definition approves the profile and its binding and whose
/// settings select the profile; `timeout` is the model spec's per-attempt bound.
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
    let mut fx = AuthorityFixture::new(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn()),
        AgentToolAuthority::new(registry).with_model_profiles(catalog(with_binding)),
        Some(spec),
    )
    .with_envelope(envelope)
    .with_credential_resolver("sk-live-sentinel");
    fx.settings = AgentSettings::default().with_change(AgentSettingsChange::ModelProfile(profile_id()));
    fx
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
    let mut fx = AuthorityFixture::new(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn()),
        AgentToolAuthority::new(registry).with_model_profiles(Arc::new(StaticAgentModelProfileCatalog::new())),
        None,
    )
    .with_envelope(envelope);
    fx.settings = AgentSettings::default().with_change(AgentSettingsChange::ModelProfile(profile_id()));
    fx.start().await;
    fx.pump().await;
    assert_eq!(fx.terminal_failure_code().await, "model-profile-unknown");
    assert_eq!(fx.adapter.calls(), 0, "no model is asked under an unknown profile");
}

/// A profile whose binding the current settings revoke refuses the model
/// call exactly as a revoked tool binding does.
#[tokio::test]
async fn a_revoked_profile_binding_refuses_the_model_call() {
    let mut fx = profiled_fixture(true, Some(30_000));
    fx.settings = fx
        .settings
        .clone()
        .with_change(AgentSettingsChange::RevokeCredentialBinding(binding()));
    fx.start().await;
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
    fx.pump().await;
    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(run.status, AgentRunStatus::Completed);
    assert_eq!(fx.adapter.calls(), 1);
}
```

`AgentSettings::with_change` and `fx.settings` are the fixture's way to select a profile; if the fixture exposes settings differently (for example `with_settings(AgentSettings)` or an `AgentEntityCommand::UpdateSettings` helper), use that path — the requirement is that the agent's *current settings revision* selects `PROFILE`, so that `authorize_model` reads it from `context.settings.settings().model_profile`. `ScriptedCredentialResolver::resolutions()` exists on the testkit type (`testkit.rs:3402` region); if it is named differently, use the accessor the type has.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p rakka-agent --test model_provider_dispatch`
Expected: compile error, `with_model_profiles` does not exist.

- [ ] **Step 3: Add the catalog to the authority and the grant fields**

In `tools.rs`:

Add the field `model_profiles: Option<Arc<dyn AgentModelProfileCatalog>>` to `AgentToolAuthority` (doc: "The deployment's model profile catalog, when one is installed; without it a profile is an opaque approved id, as before."), initialise it `None` in `new`, and add after `with_a2a_guardrails`:

```rust
    /// Installs the deployment's model profile catalog.
    ///
    /// With a catalog, [`Self::authorize_model`] resolves the selected
    /// profile's record: an unknown id refuses `model-profile-unknown`; the
    /// record's credential binding passes the same envelope and revocation
    /// checks a tool's binding does and is put on the grant, so the dispatcher
    /// resolves it inside the attempt with no new path; the record's revision
    /// and digest are recorded on the grant; and a credential-bearing model
    /// call whose intent carries no `timeout_ms` is refused
    /// `model-timeout-unset`, because the resolver's only deadline input is
    /// the effect's own timeout ([Phase 7 design 4.2](../../../docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md)).
    #[must_use]
    pub fn with_model_profiles(mut self, catalog: Arc<dyn AgentModelProfileCatalog>) -> Self {
        self.model_profiles = Some(catalog);
        self
    }
```

Add to `AgentGrantedDispatch`:

```rust
    /// The credential binding the selected model profile names, when the
    /// authority resolved one; the dispatcher resolves it when the intent
    /// itself carries none.
    pub model_credential_binding: Option<AgentCredentialBindingRef>,
    /// The descriptors the model may be shown for this call: registered,
    /// declared by the envelope, not revoked by the current settings, and
    /// narrowed by the run's setup when one exists.
    pub tools: Vec<AgentToolDescriptor>,
```

and to `AgentDispatchGrant`:

```rust
    /// The revision of the model profile record that authorized a model
    /// call, when a catalog resolved one.
    #[serde(default)]
    pub model_profile_revision: Option<AgentRevisionNumber>,
    /// The digest of that record, for the attempt's audit trail.
    #[serde(default)]
    pub model_profile_digest: Option<AgentContentDigest>,
```

Every site that constructs `AgentGrantedDispatch` or `AgentDispatchGrant` by literal (in `authorize_model`, `authorize_tool`, `grant()`, and any test double in `tests/common/mod.rs`) gains the new fields with `None`/`Vec::new()`; `grep -rn "AgentGrantedDispatch {\|AgentDispatchGrant {" crates/rakka-agent` lists them.

In `authorize_model`, after the existing envelope checks on `profile` and before `if let Some(credential) = &intent.credential_binding`, add:

```rust
        // The profile record, when a catalog is installed: its binding is
        // checked exactly as a tool's, its revision and digest ride the
        // grant, and a credential-bearing call must carry the attempt bound
        // the resolver derives its deadline from.
        let mut model_credential_binding = None;
        let mut model_profile_revision = None;
        let mut model_profile_digest = None;
        if let (Some(catalog), Some(profile_id)) = (&self.model_profiles, &profile) {
            let Some(record) = catalog.profile(profile_id) else {
                return Err(AgentAuthorityRefusal::of(
                    "model-profile-unknown",
                    format!("the deployment's model profile catalog knows no profile {profile_id}"),
                ));
            };
            if let Some(binding) = &record.credential_binding {
                self.check_credential(context, binding)?;
                if intent.timeout_ms.is_none() {
                    return Err(AgentAuthorityRefusal::of(
                        "model-timeout-unset",
                        format!(
                            "the model profile {profile_id} names the credential binding {binding}, \
                             and the model effect's spec carries no timeout_ms; a resolver's only \
                             deadline input is the effect's own timeout"
                        ),
                    ));
                }
                model_credential_binding = Some(binding.clone());
            }
            model_profile_revision = Some(record.revision);
            model_profile_digest = Some(record.digest());
        }
```

Compute the model-visible tools after the guardrail block and before the `Ok(AgentGrantedDispatch { … })`:

```rust
        let mut tools: Vec<AgentToolDescriptor> = self
            .registry
            .model_visible(context.definition.envelope(), settings)
            .into_iter()
            .cloned()
            .collect();
        if let Some(setup) = context.setup {
            tools.retain(|descriptor| setup.envelope().tools.contains_key(&descriptor.tool));
        }
```

Then build the grant with the profile fields and binding: replace `grant: self.grant(context, scope, task, goal, intent, None, BTreeSet::new(), now),` with

```rust
            grant: {
                let mut grant = self.grant(context, scope, task, goal, intent, None, BTreeSet::new(), now);
                if grant.credential_binding.is_none() {
                    grant.credential_binding = model_credential_binding.clone();
                }
                grant.model_profile_revision = model_profile_revision;
                grant.model_profile_digest = model_profile_digest;
                grant
            },
```

and add `model_credential_binding,` and `tools,` to the `AgentGrantedDispatch` literal; `authorize_tool` sets `model_credential_binding: None, tools: Vec::new()`. Import `AgentModelProfileCatalog` from `crate::model_profile` and `AgentContentDigest` if not already in scope. `grant()` initialises the two new `AgentDispatchGrant` fields to `None`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent --test model_provider_dispatch --test tool_authority --test model_response_guardrails` then `cargo test -p rakka-agent`, `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`.
Expected: the four tests pass. The credential-free case must complete, which proves an installed catalog changes nothing for a profile without a binding beyond recording its revision and digest.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent/src/tools.rs crates/rakka-agent/tests/model_provider_dispatch.rs crates/rakka-agent/tests/common/mod.rs
git commit -m "Resolve the model profile at the authority: its binding, revision, digest, and the model-visible tools ride the grant

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: `call_with`, the credential from the grant, the per-attempt deadline, and the tool list on the request

**Files:**
- Modify: `crates/rakka-agent/src/model.rs` (`AgentModelAdapter` at 604; `AgentModelRequest` at 380 and its builders at 425–450)
- Modify: `crates/rakka-agent/src/dispatch.rs` (`AgentEffectCredentialResolver` doc at 1346; `attempt_invocation` credential resolution at ~2678–2752; the Model arm at ~3519–3549)
- Modify: `crates/rakka-agent/src/model_profile.rs` (the router's `call_with` forwarding from Task 1's listing)
- Modify: `crates/rakka-agent/src/testkit.rs` (`DeterministicModelAdapter` at 1856; `ScriptedCredentialResolver` at 3402)
- Modify: `crates/rakka-agent/tests/model_provider_dispatch.rs` (dispatcher half), `crates/rakka-agent/tests/secret_exclusion.rs` (one profiled scenario)

**Interfaces:**
- Consumes: Task 2's `AgentGrantedDispatch.{model_credential_binding, tools}`, `AgentDispatchGrant.credential_binding`; `AgentRunEffect: Clone` with `timeout_ms`, `deadline_at`; `AgentTimestampMillis::{new, as_millis}`.
- Produces:
  - `AgentModelAdapter::call_with<'a>(&'a self, request: &'a AgentModelRequest, credential: Option<&'a AgentEphemeralCredential>) -> AgentModelFuture<'a> { self.call(request) }` (defaulted).
  - `AgentModelRequest.tools: Vec<AgentToolDescriptor>` (`#[serde(default)]`) and `with_tools(self, tools: Vec<AgentToolDescriptor>) -> Self`.
  - The dispatcher: credential binding `intent.credential_binding.clone().or_else(|| granted.grant.credential_binding.clone())`; a per-attempt `attempt_intent` clone with `deadline_at = Some(started_at + timeout_ms)` when `timeout_ms` is `Some`, handed to `resolve`, `execute`, and `call_with`; `request.with_tools(granted.tools.clone())`.
  - Testkit: `DeterministicModelAdapter::credentials_seen(&self) -> Vec<Option<&'static str>>` (the material kind label per call, never a value) and `ScriptedCredentialResolver::deadlines(&self) -> Vec<Option<AgentTimestampMillis>>`.

- [ ] **Step 1: Write the failing tests**

Append to `crates/rakka-agent/tests/model_provider_dispatch.rs`:

```rust
// ---------------------------------------------------------------------------
// The dispatcher: the credential path, the deadline, and the tool list.
// ---------------------------------------------------------------------------

/// The profile's binding is resolved inside the attempt through the existing
/// resolver and handed to `call_with`; the request carries the model-visible
/// tools; the resolver sees a deadline derived from the attempt bound.
#[tokio::test]
async fn the_profile_credential_reaches_call_with_under_the_attempt_deadline() {
    let fx = profiled_fixture(true, Some(30_000));
    fx.start().await;
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
    let request = fx.adapter.requests().into_iter().next().expect("one request");
    assert!(
        deadline.as_millis() >= 30_000,
        "the deadline is the attempt start plus the spec's timeout: {deadline:?}"
    );
    assert_eq!(
        request.tools.iter().map(|tool| tool.tool.as_str()).collect::<Vec<_>>(),
        vec![TOOL],
        "the request carries the model-visible tool list"
    );
}

/// A tool the settings revoke is withheld from the request, not offered and
/// refused later.
#[tokio::test]
async fn a_revoked_tool_is_withheld_from_the_model_visible_list() {
    let mut fx = profiled_fixture(false, None);
    fx.settings = fx.settings.clone().with_change(AgentSettingsChange::RevokeTool(
        rakka_agent::AgentToolId::new(TOOL).expect("tool id"),
    ));
    fx.start().await;
    fx.pump().await;
    let request = fx.adapter.requests().into_iter().next().expect("one request");
    assert!(request.tools.is_empty(), "revoked tools never reach the model: {:?}", request.tools);
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
    let context = rakka_agent::AgentContextSnapshotRef::new("snapshot-1").expect("ref");
    let request = AgentModelRequest::new(context, 1);
    let turn = CallOnly.call_with(&request, Some(&credential)).await.expect("forwards to call");
    assert_eq!(turn.text.as_deref(), Some("Done."));
}
```

Append to `crates/rakka-agent/tests/secret_exclusion.rs`, using that file's own helpers (`credentialed_fixture`, `assert_no_secret_anywhere`, `SECRETS`), one scenario:

```rust
/// A model call whose credential comes from a profile's binding leaves the
/// credential on no durable record, log, or metric — the same sweep the
/// tool-credential scenarios run, over a profiled run.
#[tokio::test]
async fn a_profile_resolved_model_credential_reaches_no_durable_record() {
    use rakka_agent::{
        AgentModelCapabilities, AgentModelProfile, AgentModelProfileId, AgentModelProviderKind,
        AgentSamplingSettings, AgentSettings, AgentSettingsChange, StaticAgentModelProfileCatalog,
    };
    let profile_id = AgentModelProfileId::new("profiled").expect("profile id");
    let registry = tool_registry_for_spec(TOOL, &credentialed_spec());
    let mut envelope = envelope_for_registry(&registry);
    envelope.model_profiles.insert(profile_id.clone());
    let catalog = StaticAgentModelProfileCatalog::new()
        .with_profile(AgentModelProfile {
            profile_id: profile_id.clone(),
            revision: AgentRevisionNumber::INITIAL,
            provider: AgentModelProviderKind::Anthropic,
            model: "claude-sonnet-5".to_string(),
            base_url: None,
            credential_binding: credentialed_spec().credential_binding.clone(),
            default_sampling: AgentSamplingSettings::default(),
            capabilities: AgentModelCapabilities { tool_calls: true },
            attributes: Default::default(),
        })
        .expect("valid");
    let mut fx = AuthorityFixture::new(
        DeterministicModelAdapter::new().with_turn_for(1, proposing_turn()),
        AgentToolAuthority::new(registry).with_model_profiles(std::sync::Arc::new(catalog)),
        Some(AgentEffectSpec::read_only().with_timeout_ms(30_000)),
    )
    .with_envelope(envelope)
    .with_credential_resolver(SECRETS[0]);
    fx.settings = AgentSettings::default().with_change(AgentSettingsChange::ModelProfile(profile_id));
    fx.start().await;
    fx.pump().await;
    assert_eq!(fx.adapter.credentials_seen(), vec![Some("bearer-token")]);
    assert_no_secret_anywhere(&fx).await;
}
```

Adapt the helper names to what `secret_exclusion.rs` actually defines (`credentialed_spec`, `proposing_turn`, `tool_registry_for_spec`, `envelope_for_registry`, `SECRETS`, `assert_no_secret_anywhere`); the imports the file already has cover most of these.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p rakka-agent --test model_provider_dispatch`
Expected: compile errors: `call_with`, `credentials_seen`, `deadlines`, `request.tools` do not exist.

- [ ] **Step 3: The trait and the request**

In `model.rs`, add to `AgentModelAdapter` after `call`:

```rust
    /// Performs one bounded model call with the credential the dispatcher
    /// resolved for this attempt, when the effect's grant named a binding.
    ///
    /// The dispatcher calls this and only this. The default forwards to
    /// [`Self::call`], so an adapter that takes its credentials at
    /// construction keeps working unchanged; an adapter that builds a
    /// provider client per attempt overrides it, reads the material for the
    /// duration of the call, and holds nothing afterwards. The credential is
    /// the one [`crate::dispatch::AgentEffectCredentialResolver::resolve`]
    /// produced inside the attempt, under the effect's own deadline; an
    /// adapter never resolves a credential itself and never holds a resolver.
    fn call_with<'a>(
        &'a self,
        request: &'a AgentModelRequest,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentModelFuture<'a> {
        let _ = credential;
        self.call(request)
    }
```

(import `rakka_agent_workflow::AgentEphemeralCredential`). Add to `AgentModelRequest`:

```rust
    /// The descriptors the model may be shown for this call: the registered
    /// tools the envelope declares, minus those the current settings revoke,
    /// narrowed by the run's setup — the authority's `model_visible`
    /// derivation, carried here so the adapter declares each beside the
    /// result tool. Visibility is not authority: a call the model makes still
    /// needs a grant to dispatch.
    #[serde(default)]
    pub tools: Vec<crate::tools::AgentToolDescriptor>,
```

initialised `Vec::new()` in `new`, with:

```rust
    /// Carries the model-visible tool list.
    #[must_use]
    pub fn with_tools(mut self, tools: Vec<crate::tools::AgentToolDescriptor>) -> Self {
        self.tools = tools;
        self
    }
```

In `model_profile.rs`, add the router's `call_with` forwarding exactly as Task 1's listing shows it.

- [ ] **Step 4: The dispatcher**

In `dispatch.rs`, extend the `AgentEffectCredentialResolver` doc with: "The effect handed over carries two time fields: `timeout_ms`, the attempt bound its spec declared, and `deadline_at`, which the dispatcher stamps per attempt as the attempt's start plus that bound and never persists. A resolver derives the lease it asks for from `deadline_at`; when both are `None` the effect declared no bound, and a resolver may only ask for its minimum lease — which is why a credential-bearing model call without a timeout is refused at the authority."

In `attempt_invocation`, replace the credential resolution's binding source and stamp the deadline. Before `let credential = match &intent.credential_binding {`, add:

```rust
        // The attempt bound, stamped once per attempt and never persisted:
        // a durable deadline would outlive the generation it bounds.
        let attempt_started_at = self.now();
        let mut attempt_intent = intent.clone();
        attempt_intent.deadline_at = intent.timeout_ms.map(|timeout_ms| {
            AgentTimestampMillis::new(attempt_started_at.as_millis().saturating_add(timeout_ms))
        });
        let attempt_intent = &attempt_intent;
        // The binding: the intent's own, or the one the grant carries from
        // the selected model profile.
        let credential_binding = intent
            .credential_binding
            .clone()
            .or_else(|| granted.grant.credential_binding.clone());
```

change `match &intent.credential_binding {` to `match &credential_binding {`, change `resolver.resolve(scope, binding, intent).await` to `resolver.resolve(scope, binding, attempt_intent).await`, and change `self.invoke(scope, intent, &granted, credential.as_ref())` to `self.invoke(scope, attempt_intent, &granted, credential.as_ref())`. Everything that records or settles (the `settle_undispatchable`, `record_attempt_failure`, and later calls) keeps `intent`, the durable record.

In the Model arm, after `request = request.with_profile(profile);` add `request = request.with_tools(granted.tools.clone());` and replace `let called = self.model.call(&request).await;` with `let called = self.model.call_with(&request, credential).await;`.

- [ ] **Step 5: The testkit**

In `testkit.rs`, give `DeterministicModelAdapter` a field `credentials: Arc<Mutex<Vec<Option<&'static str>>>>` (initialised in `new`), an accessor:

```rust
    /// The material kind of the credential each call was handed, in call
    /// order — a label, never a value.
    #[must_use]
    pub fn credentials_seen(&self) -> Vec<Option<&'static str>> {
        self.credentials.lock().expect("the credential log should not be poisoned").clone()
    }
```

and an `AgentModelAdapter::call_with` override that pushes `credential.map(|credential| credential.material().kind_label())` and then produces the turn exactly as `call` does. `AgentEphemeralCredentialMaterial::kind_label` (`rakka-agent-workflow/src/credentials.rs:230`) answers labels such as `bearer-token`; use it, and assert the label the test expects from `AgentEphemeralCredential::bearer_token`.

Give `ScriptedCredentialResolver` a field `deadlines: Arc<Mutex<Vec<Option<AgentTimestampMillis>>>>`, push `effect.deadline_at` in `resolve` before answering, and add `pub fn deadlines(&self) -> Vec<Option<AgentTimestampMillis>>`. If `resolutions()` is not yet public, add it as the count accessor.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p rakka-agent --test model_provider_dispatch --test secret_exclusion --test model_profile_catalog --test effect_dispatch --test tool_authority` then `cargo test -p rakka-agent`, `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`.
Expected: all pass, including every pre-existing tool-credential scenario (their binding now flows through `credential_binding` unchanged, since the intent's own binding wins).

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent/src/model.rs crates/rakka-agent/src/dispatch.rs crates/rakka-agent/src/model_profile.rs crates/rakka-agent/src/testkit.rs crates/rakka-agent/tests/model_provider_dispatch.rs crates/rakka-agent/tests/secret_exclusion.rs
git commit -m "Hand the adapter the credential the dispatcher resolved, under a per-attempt deadline, with the model-visible tools on the request

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: The provider telemetry slots on the turn, the usage, the segment, and the OTel mapping

**Files:**
- Modify: `crates/rakka-agent/src/model.rs` (`AgentModelUsage` at 105; `AgentModelTurn` at 193, its builders, `validate` at 279, the shadow record at 330 and the `Deserialize` impl at 346; `AgentModelError`)
- Modify: `crates/rakka-agent/src/tools.rs` (the 7.2 transform guard in `review_model_response`: the two new turn fields join the immutable set)
- Modify: `crates/rakka-agent/src/observability.rs` (`AgentTelemetrySegment` at ~1829, beside `usage`)
- Modify: `crates/rakka-agent/src/dispatch.rs` (~2802, where `attempt_segment.usage(turn.usage)` is set)
- Modify: `crates/rakka-agent/src/otel.rs` (constants near 100–121; `AGENT_SPAN_ATTRIBUTE_KEYS` at 178; `segment_span` near 750; `usage_attributes` at 907)
- Modify: `crates/rakka-agent/src/lib.rs` (re-export `AgentModelResponseMetadata`, `AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES`, and under the `otel` re-exports the four attribute constants)
- Modify: `crates/rakka-agent/tests/otel_span_mapping.rs`, `crates/rakka-agent/tests/model_response_guardrails.rs`

**Interfaces:**
- Produces:
  - `AgentModelUsage` gains `#[serde(default, skip_serializing_if = "Option::is_none")] pub cached_input_tokens: Option<u64>` and `pub reasoning_tokens: Option<u64>` (the struct stays `Copy + Default`).
  - `AgentModelTurn` gains `#[serde(default, skip_serializing_if = "Option::is_none")] pub response_model: Option<String>` and `pub finish_reason: Option<String>`, builders `with_response_model(self, impl Into<String>)` and `with_finish_reason(self, impl Into<String>)`, and `validate` refuses either over `AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES = 128` with `AgentModelError::ResponseMetadataTooLong { field: &'static str, bytes: usize, maximum: usize }` (code `model-response-metadata-too-long`).
  - `pub struct AgentModelResponseMetadata { pub model: Option<String>, pub finish_reason: Option<String> }` with `AgentModelResponseMetadata::bounded(model: Option<String>, finish_reason: Option<String>) -> Self` that truncates each to the bound at a char boundary (so an adapter can never produce an unbounded turn from provider text).
  - `AgentTelemetrySegment` gains `#[serde(default)] pub model_response: Option<AgentModelResponseMetadata>` and a builder `model_response(self, metadata: AgentModelResponseMetadata) -> Self` mirroring `usage`.
  - OTel: `ATTR_GEN_AI_RESPONSE_MODEL = "gen_ai.response.model"`, `ATTR_GEN_AI_RESPONSE_FINISH_REASONS = "gen_ai.response.finish_reasons"`, `ATTR_RAKKA_AGENT_MODEL_CACHED_INPUT_TOKENS = "rakka.agent.model.cached_input_tokens"`, `ATTR_RAKKA_AGENT_MODEL_REASONING_TOKENS = "rakka.agent.model.reasoning_tokens"`, all four in `AGENT_SPAN_ATTRIBUTE_KEYS`; `usage_attributes` writes the two token keys when `Some(n)` with `n > 0`; `segment_span` writes the two response keys when the segment's `model_response` carries them.

- [ ] **Step 1: Write the failing tests**

Append to `crates/rakka-agent/tests/otel_span_mapping.rs` (its imports already bring `segment`, `AgentSegmentOperation`, `segment_span`, and the attribute constants; add the four new constants and `AgentModelResponseMetadata`, `AgentModelUsage` to the `use rakka_agent::{…}` list):

```rust
/// The provider fields 17.8 owed have slots, and the mapping writes them only
/// when the provider reported them.
#[test]
fn provider_response_fields_are_written_when_reported_and_omitted_when_not() {
    let mut carried = segment(AgentSegmentOperation::ModelInference {
        model_profile: Some("anthropic-sonnet".to_string()),
    })
    .usage(AgentModelUsage {
        input_tokens: 10,
        output_tokens: 5,
        cost_micros: 0,
        cached_input_tokens: Some(3),
        reasoning_tokens: Some(2),
    })
    .model_response(AgentModelResponseMetadata::bounded(
        Some("claude-sonnet-5-20260901".to_string()),
        Some("end_turn".to_string()),
    ));
    let span = segment_span(&carried).expect("maps");
    assert_eq!(span.attributes.get(ATTR_GEN_AI_RESPONSE_MODEL).map(String::as_str), Some("claude-sonnet-5-20260901"));
    assert_eq!(span.attributes.get(ATTR_GEN_AI_RESPONSE_FINISH_REASONS).map(String::as_str), Some("end_turn"));
    assert_eq!(span.attributes.get(ATTR_RAKKA_AGENT_MODEL_CACHED_INPUT_TOKENS).map(String::as_str), Some("3"));
    assert_eq!(span.attributes.get(ATTR_RAKKA_AGENT_MODEL_REASONING_TOKENS).map(String::as_str), Some("2"));

    carried.model_response = None;
    carried.usage = Some(AgentModelUsage {
        input_tokens: 10,
        output_tokens: 5,
        cost_micros: 0,
        cached_input_tokens: Some(0),
        reasoning_tokens: None,
    });
    let span = segment_span(&carried).expect("maps");
    for key in [
        ATTR_GEN_AI_RESPONSE_MODEL,
        ATTR_GEN_AI_RESPONSE_FINISH_REASONS,
        ATTR_RAKKA_AGENT_MODEL_CACHED_INPUT_TOKENS,
        ATTR_RAKKA_AGENT_MODEL_REASONING_TOKENS,
    ] {
        assert!(!span.attributes.contains_key(key), "{key} is never invented");
    }
}
```

If `AgentTelemetrySegment`'s fields are private and set only through builders, keep `.model_response(..)`/`.usage(..)` calls and build a second segment instead of mutating.

Add a unit test in `model.rs`'s existing `#[cfg(test)] mod tests` (create one if absent):

```rust
    #[test]
    fn response_metadata_is_bounded_on_the_turn_and_truncated_by_the_helper() {
        let turn = AgentModelTurn::new(AgentRevisionNumber::INITIAL)
            .with_response_model("m".repeat(AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES + 1));
        assert_eq!(turn.validate().expect_err("too long").code(), "model-response-metadata-too-long");
        let bounded = AgentModelResponseMetadata::bounded(Some("m".repeat(500)), Some("é".repeat(200)));
        assert_eq!(bounded.model.as_deref().map(str::len), Some(AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES));
        assert!(bounded.finish_reason.as_deref().map_or(false, |s| s.len() <= AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES && s.is_char_boundary(s.len())));
        let encoded = serde_json::to_string(&AgentModelTurn::new(AgentRevisionNumber::INITIAL)).expect("encodes");
        assert!(!encoded.contains("response_model"), "absent fields are not serialized: {encoded}");
        let decoded: AgentModelTurn = serde_json::from_str(&encoded).expect("a pre-field record decodes");
        assert!(decoded.response_model.is_none() && decoded.usage.cached_input_tokens.is_none());
    }
```

Append to `crates/rakka-agent/tests/model_response_guardrails.rs` (its stages and helpers exist):

```rust
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

#[test]
fn a_transform_that_rewrites_the_response_model_is_refused_as_invalid() {
    let turn = text_turn("hello").with_response_model("claude-sonnet-5");
    let refusal = authority_with(Arc::new(RewriteResponseModel))
        .review_model_response(&run_scope(), turn)
        .expect_err("provider provenance is immutable");
    assert_eq!(refusal.code, "guardrail-transform-invalid");
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p rakka-agent --features otel --test otel_span_mapping` and `cargo test -p rakka-agent --lib model::tests` and `cargo test -p rakka-agent --test model_response_guardrails`
Expected: compile errors on the new fields, constants, and builders.

- [ ] **Step 3: The turn and the usage**

In `model.rs`:

```rust
/// Longest provider-reported response model name or finish reason a turn carries, in bytes.
pub const AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES: usize = 128;

/// What a provider reported about its answer beyond the content: the model
/// that actually answered and why it stopped. Observability only, never read
/// for inference; bounded so a turn stays within its record limits.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AgentModelResponseMetadata {
    /// The provider's response model name, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The provider's finish or stop reason, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

fn truncate_at_boundary(mut value: String, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}

impl AgentModelResponseMetadata {
    /// Metadata with each field truncated to [`AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES`]
    /// at a character boundary, so provider text can never produce an
    /// unbounded turn.
    #[must_use]
    pub fn bounded(model: Option<String>, finish_reason: Option<String>) -> Self {
        Self {
            model: model.map(|value| truncate_at_boundary(value, AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES)),
            finish_reason: finish_reason
                .map(|value| truncate_at_boundary(value, AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES)),
        }
    }
}
```

`AgentModelUsage`: add the two fields with docs ("Input tokens the provider served from its cache, when it reported them; observability, never billing", "Reasoning tokens the provider reported, when it did"), `#[serde(default, skip_serializing_if = "Option::is_none")]`; `total_tokens` is unchanged. `AgentModelTurn`: add `response_model` and `finish_reason` with the same attributes and docs ("provenance, never inference"), initialise `None` in `new`, add the two builders, add both to `AgentModelTurnRecord` (`#[serde(default)]`) and to the `Deserialize` impl, and in `validate` add:

```rust
        for (field, value) in [("response_model", &self.response_model), ("finish_reason", &self.finish_reason)] {
            if let Some(value) = value {
                if value.len() > AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES {
                    return Err(AgentModelError::ResponseMetadataTooLong {
                        field,
                        bytes: value.len(),
                        maximum: AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES,
                    });
                }
            }
        }
```

with the variant, its `code()` arm (`model-response-metadata-too-long`), and its `Display` arm. Re-export `AgentModelResponseMetadata` and `AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES` from `lib.rs`.

In `tools.rs`, extend the 7.2 immutable-set clause in `review_model_response` so it also refuses when `transformed.response_model != review.turn.response_model || transformed.finish_reason != review.turn.finish_reason`, and extend the message and the method doc ("…adapter version, model profile, usage, response model, or finish reason").

- [ ] **Step 4: The segment and the mapping**

In `observability.rs`, add to `AgentTelemetrySegment` beside `usage`:

```rust
    /// What the provider reported about its answer, when the segment carried it.
    #[serde(default)]
    pub model_response: Option<crate::model::AgentModelResponseMetadata>,
```

initialise `None` wherever the struct is built (the `AgentSegmentTimer::close` path and any literal), and a builder next to `usage`:

```rust
    /// Attaches the provider's response metadata.
    #[must_use]
    pub fn model_response(mut self, metadata: crate::model::AgentModelResponseMetadata) -> Self {
        self.model_response = Some(metadata);
        self
    }
```

In `dispatch.rs` at the `Ok(AgentRunEffectOutcome::Model { turn }) => attempt_segment.usage(turn.usage)` arm, attach the metadata too:

```rust
            Ok(AgentRunEffectOutcome::Model { turn }) => attempt_segment
                .usage(turn.usage)
                .model_response(AgentModelResponseMetadata {
                    model: turn.response_model.clone(),
                    finish_reason: turn.finish_reason.clone(),
                }),
```

(adapt to the expression's exact shape; the intent is that the attempt segment carries both).

In `otel.rs`: add the four constants with docs beside their neighbours (`gen_ai.response.model` and `gen_ai.response.finish_reasons` are GenAI semantic-convention names; the two token keys are Rakka-namespaced because the convention revision the crate pins has no stable name for them), add all four to `AGENT_SPAN_ATTRIBUTE_KEYS` in sorted position, extend `usage_attributes`:

```rust
    for (key, tokens) in [
        (ATTR_RAKKA_AGENT_MODEL_CACHED_INPUT_TOKENS, usage.cached_input_tokens),
        (ATTR_RAKKA_AGENT_MODEL_REASONING_TOKENS, usage.reasoning_tokens),
    ] {
        if let Some(tokens) = tokens {
            if tokens > 0 {
                attributes.insert(key.to_string(), tokens.to_string());
            }
        }
    }
```

and in `segment_span`, after the usage block:

```rust
    if let Some(metadata) = &segment.model_response {
        if let Some(model) = &metadata.model {
            attributes.insert(ATTR_GEN_AI_RESPONSE_MODEL.to_string(), model.clone());
        }
        if let Some(reason) = &metadata.finish_reason {
            attributes.insert(ATTR_GEN_AI_RESPONSE_FINISH_REASONS.to_string(), reason.clone());
        }
    }
```

Re-export the four constants where the other `ATTR_GEN_AI_*` constants are re-exported under the `otel` feature.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p rakka-agent --all-features --test otel_span_mapping --test model_response_guardrails --test telemetry_segments --test trace_scenarios --test schema_compatibility --test compatibility_currency` and `cargo test -p rakka-agent --lib` then `cargo test -p rakka-agent --all-features`, `cargo check -p rakka-agent --no-default-features`, `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`.
Expected: all pass; `the_allowlist_accepts_exactly_what_it_declares` accepts the four new keys because they are in the constant; the schema-version tables are untouched (no version moves).

- [ ] **Step 6: Commit**

```bash
git add crates/rakka-agent/src/model.rs crates/rakka-agent/src/tools.rs crates/rakka-agent/src/observability.rs crates/rakka-agent/src/dispatch.rs crates/rakka-agent/src/otel.rs crates/rakka-agent/src/lib.rs crates/rakka-agent/tests/otel_span_mapping.rs crates/rakka-agent/tests/model_response_guardrails.rs
git commit -m "Give the provider's response model, finish reason, and cached and reasoning tokens slots on the turn and the span

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: The `rig` feature gains `rustls` only; the Rig adapter declares the visible tools, maps the new usage fields, and takes an optional response-metadata extractor

**Files:**
- Modify: `crates/rakka-agent/Cargo.toml:40-44` (the `rig-core` line and its comment)
- Modify: `crates/rakka-agent/tests/crate_shape.rs` (one new test)
- Modify: `crates/rakka-agent/src/rig.rs` (module doc's pin-review note; `RigModelAdapter` fields and builders at 88–140; `build_request` at 151; `turn_from_response` at 183; `model_usage` at 280; unit tests at 480+)

**Interfaces:**
- Consumes: Task 3's `AgentModelRequest.tools`; Task 4's `AgentModelUsage.{cached_input_tokens, reasoning_tokens}`, `AgentModelTurn::{with_response_model, with_finish_reason}`, `AgentModelResponseMetadata::bounded`.
- Produces:
  - Manifest: `rig-core = { version = "=0.37.0", default-features = false, features = ["rustls"], optional = true }`.
  - `RigModelAdapter::with_response_metadata(self, extractor: fn(&M::Response) -> AgentModelResponseMetadata) -> Self` (a plain `fn` pointer; the struct stays `Debug + Clone`).
  - `pub fn anthropic_response_metadata(response: &rig_core::providers::anthropic::completion::CompletionResponse) -> AgentModelResponseMetadata` and `pub fn openai_completions_response_metadata(response: &rig_core::providers::openai::completion::CompletionResponse) -> AgentModelResponseMetadata`, both in `rig.rs`.

- [ ] **Step 1: Write the failing tests**

In `crates/rakka-agent/tests/crate_shape.rs` add:

```rust
/// The Rig pin selects a TLS backend and nothing else: rig's `reqwest` feature
/// would enable `reqwest/system-proxy`, an environment-proxy egress bypass
/// that Cargo feature unification would switch on for every crate in a
/// consumer's graph.
#[test]
fn rig_core_enables_rustls_only_and_never_reqwest() {
    let manifest = read("crates/rakka-agent/Cargo.toml");
    let line = manifest
        .lines()
        .find(|line| line.trim_start().starts_with("rig-core"))
        .expect("the rig-core dependency line exists");
    assert!(line.contains("default-features = false"), "{line}");
    assert!(line.contains("features = [\"rustls\"]"), "{line}");
    assert!(!line.contains("\"reqwest\""), "rig's reqwest feature is never enabled: {line}");
}
```

In `rig.rs`'s `#[cfg(test)] mod tests`, extend the existing request-shape test or add beside it (the module already has `request()`, `ScriptedCompletionModel`, and a way to inspect the built `CompletionRequest`; follow `the_result_tool_and_sampling_reach_the_completion_request`):

```rust
    #[test]
    fn the_model_visible_tools_are_declared_beside_the_result_tool() {
        let descriptor = crate::tools::AgentToolDescriptor::new(
            AgentToolId::new("search_kb").expect("tool id"),
            crate::tools::AgentToolKind::Function,
            "Searches the knowledge base.",
            crate::definition::AgentSchemaRef::new(
                crate::definition::AgentSchemaId::new("kb-input").expect("schema id"),
                AgentRevisionNumber::INITIAL,
            ),
            crate::definition::AgentSchemaRef::new(
                crate::definition::AgentSchemaId::new("kb-output").expect("schema id"),
                AgentRevisionNumber::INITIAL,
            ),
        )
        .expect("descriptor")
        .with_parameters(serde_json::json!({ "type": "object", "properties": { "q": { "type": "string" } } }))
        .expect("parameters");
        let adapter = RigModelAdapter::new(ScriptedCompletionModel::new());
        let built = adapter.build_request(&request().with_tools(vec![descriptor]));
        let names: Vec<&str> = built.tools.iter().map(|tool| tool.name.as_str()).collect();
        assert!(names.contains(&AGENT_RESULT_TOOL_DEFAULT));
        assert!(names.contains(&"search_kb"));
        let declared = built.tools.iter().find(|tool| tool.name == "search_kb").expect("declared");
        assert_eq!(declared.description, "Searches the knowledge base.");
        assert_eq!(declared.parameters["properties"]["q"]["type"], "string");
    }

    #[test]
    fn cached_and_reasoning_tokens_ride_the_usage_when_reported() {
        let usage = Usage {
            input_tokens: 10,
            output_tokens: 5,
            total_tokens: 15,
            cached_input_tokens: 3,
            cache_creation_input_tokens: 0,
            reasoning_tokens: 2,
        };
        let mapped = model_usage(&usage);
        assert_eq!(mapped.cached_input_tokens, Some(3));
        assert_eq!(mapped.reasoning_tokens, Some(2));
        let silent = model_usage(&Usage { cached_input_tokens: 0, reasoning_tokens: 0, ..usage });
        assert_eq!(silent.cached_input_tokens, None, "a zero is not a report");
        assert_eq!(silent.reasoning_tokens, None);
    }

    #[tokio::test]
    async fn an_installed_extractor_fills_the_turns_response_metadata() {
        fn extract(_: &<ScriptedCompletionModel as CompletionModel>::Response) -> AgentModelResponseMetadata {
            AgentModelResponseMetadata::bounded(Some("scripted-model".to_string()), Some("stop".to_string()))
        }
        let adapter = RigModelAdapter::new(ScriptedCompletionModel::new().returning_text("hi"))
            .with_response_metadata(extract);
        let turn = adapter.call(&request()).await.expect("answers");
        assert_eq!(turn.response_model.as_deref(), Some("scripted-model"));
        assert_eq!(turn.finish_reason.as_deref(), Some("stop"));
        let plain = RigModelAdapter::new(ScriptedCompletionModel::new().returning_text("hi"));
        let turn = plain.call(&request()).await.expect("answers");
        assert!(turn.response_model.is_none() && turn.finish_reason.is_none());
    }
```

If `Usage` does not support struct-update syntax with the fields shown, construct it field by field; if `CompletionRequest` exposes its tools under another name than `tools`, read it the way the existing request-shape test does.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p rakka-agent --test crate_shape` and `cargo test -p rakka-agent --lib rig::tests`
Expected: the manifest test fails on the missing `features = ["rustls"]`; the rig tests fail to compile.

- [ ] **Step 3: The manifest**

Change the `rig-core` line in `crates/rakka-agent/Cargo.toml` to:

```toml
rig-core = { version = "=0.37.0", default-features = false, features = ["rustls"], optional = true }
```

and extend the comment above it: "`rustls` alone: it selects a TLS backend for rig's non-optional `reqwest` 0.13 and nothing else. Rig's `reqwest` feature is never enabled — it turns on `reqwest/system-proxy`, an environment-proxy egress bypass, and Cargo feature unification would switch it on for every crate in a consumer's dependency graph. `crate_shape.rs` holds this."

- [ ] **Step 4: The adapter**

In `rig.rs`:

Add the field `response_metadata: Option<fn(&M::Response) -> AgentModelResponseMetadata>` to `RigModelAdapter<M>` (this requires `M: CompletionModel` on the struct's `impl` blocks, which they already have; on the struct itself add the bound or use `PhantomData`-free `Option<fn(&<M as CompletionModel>::Response) -> …>` inside the `impl<M: CompletionModel>` where the field is set — if the struct definition cannot name `M::Response` without a bound, add `where M: CompletionModel` to the struct), initialised `None` in `new`, with the builder:

```rust
    /// Installs an extractor that reads the provider's response model and
    /// finish reason from the raw response, for the turn's provenance slots.
    ///
    /// Rig's `CompletionResponse<T>` carries those only inside the provider's
    /// own `T`, so the adapter cannot read them generically; a provider
    /// adapter installs the extractor for the response type it knows
    /// ([`anthropic_response_metadata`], [`openai_completions_response_metadata`]).
    #[must_use]
    pub fn with_response_metadata(
        mut self,
        extractor: fn(&M::Response) -> AgentModelResponseMetadata,
    ) -> Self {
        self.response_metadata = Some(extractor);
        self
    }
```

In `build_request`, after the result tool is declared, declare each visible descriptor:

```rust
        for descriptor in &request.tools {
            builder = builder.tool(ToolDefinition {
                name: descriptor.tool.as_str().to_string(),
                description: descriptor.description.clone(),
                parameters: descriptor.parameters.clone().unwrap_or_else(|| {
                    serde_json::json!({ "type": "object", "additionalProperties": true })
                }),
            });
        }
```

(`builder` is `let mut builder = …` already.) In `turn_from_response`, before `for content in response.choice`, read the metadata from the raw response:

```rust
        if let Some(extract) = self.response_metadata {
            let metadata = extract(&response.raw_response);
            if let Some(model) = metadata.model {
                turn = turn.with_response_model(model);
            }
            if let Some(reason) = metadata.finish_reason {
                turn = turn.with_finish_reason(reason);
            }
        }
```

In `model_usage`, set `cached_input_tokens: (usage.cached_input_tokens > 0).then_some(usage.cached_input_tokens)` and `reasoning_tokens: (usage.reasoning_tokens > 0).then_some(usage.reasoning_tokens)`.

Add the two extractors:

```rust
/// Reads the response model and stop reason from an Anthropic Messages response.
#[must_use]
pub fn anthropic_response_metadata(
    response: &rig_core::providers::anthropic::completion::CompletionResponse,
) -> AgentModelResponseMetadata {
    AgentModelResponseMetadata::bounded(Some(response.model.clone()), response.stop_reason.clone())
}

/// Reads the response model and the first choice's finish reason from an
/// OpenAI Chat Completions response (also the shape OpenAI-compatible
/// endpoints answer).
#[must_use]
pub fn openai_completions_response_metadata(
    response: &rig_core::providers::openai::completion::CompletionResponse,
) -> AgentModelResponseMetadata {
    AgentModelResponseMetadata::bounded(
        Some(response.model.clone()),
        response.choices.first().map(|choice| choice.finish_reason.clone()),
    )
}
```

Extend the module doc's pin-review paragraph: the checklist now includes the provider client builders (`Client::builder`, `http_client`, `base_url`, `build`), the two raw response types the extractors read, and `Usage`'s cached and reasoning fields.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p rakka-agent --lib rig::tests` then `cargo test -p rakka-agent --test crate_shape --test compatibility_currency --features otel` then `cargo test -p rakka-agent`, `cargo check -p rakka-agent --no-default-features`, `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`, and `cargo tree -p rakka-agent -e features -i rig-core | head -20` to confirm only `rustls` (plus rig's non-optional deps) is enabled.
Expected: all pass; the tree shows no `reqwest` feature on `rig-core`.

- [ ] **Step 6: Commit**

```bash
git add crates/rakka-agent/Cargo.toml crates/rakka-agent/src/rig.rs crates/rakka-agent/tests/crate_shape.rs
git commit -m "Pin rig-core to rustls alone, declare the visible tools on the completion request, and read provider provenance through an extractor

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: `RigProviderAdapter<H>` over an injected backend, proven against in-process fake providers

**Files:**
- Modify: `crates/rakka-agent/src/rig.rs` (new struct after `RigModelAdapter`'s `impl AgentModelAdapter`)
- Modify: `crates/rakka-agent/Cargo.toml` (`[dev-dependencies]`: `axum.workspace = true`, `reqwest = { version = "0.13", default-features = false }`)
- Create: `crates/rakka-agent/tests/rig_provider_fake_endpoint.rs`

**Interfaces:**
- Consumes: Task 1's `AgentModelProfile`, `AgentModelProviderKind`, `AgentModelProfileError`; Task 3's `call_with`; Task 5's extractors and `RigModelAdapter::with_response_metadata`; rig 0.37's provider clients (verified above) and `rig_core::http_client::HttpClientExt`; `AgentEphemeralCredential::material()` → `AgentEphemeralCredentialMaterial::{BearerToken { token }, ApiKey { name, value }, Basic { .. }, Custom { .. }}`.
- Produces:
  - `pub struct RigProviderAdapter<H>` with `new(profile: AgentModelProfile, http: H) -> Result<Self, AgentModelProfileError>` (runs `validate`; refuses a `Custom` or `AzureOpenAi` profile with no `base_url` as `InvalidBaseUrl { reason: "this provider kind needs a base_url" }`), `with_adapter_version`, `with_retry_policy` (fallible, mirroring `RigModelAdapter`), `with_result_tool`; implements `AgentModelAdapter` with `call` → `call_with(request, None)`.
  - Bounds: `H: HttpClientExt + Clone + std::fmt::Debug + Default + Send + Sync + 'static` (the OpenAI completions client requires `Default`; `reqwest::Client` satisfies all).
  - Codes: `model-credential-missing` (a provider that needs a key was handed none), `model-credential-material-unsupported` (`Basic` or `Custom` material), `model-profile-invalid-base-url` (already), `model-provider-failed` for a provider client that fails to build.

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-agent/tests/rig_provider_fake_endpoint.rs`:

```rust
//! The Rig provider adapter against in-process fakes of the two provider
//! APIs it reads provenance from, over an injected HTTP backend. No network,
//! no key: the credential is an ephemeral sentinel that must appear on the
//! wire the fake sees and nowhere else.
#![cfg(feature = "rig")]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

use rakka_agent::rig::RigProviderAdapter;
use rakka_agent::{
    AgentContextSnapshotRef, AgentModelAdapter, AgentModelCapabilities, AgentModelProfile,
    AgentModelProfileId, AgentModelProviderKind, AgentModelRequest, AgentRevisionNumber,
    AgentSamplingSettings, AgentSchemaId, AgentSchemaRef, AgentToolDescriptor, AgentToolId,
    AgentToolKind,
};
use rakka_agent_workflow::AgentEphemeralCredential;

#[derive(Clone, Default)]
struct Seen {
    bodies: Arc<Mutex<Vec<Value>>>,
    headers: Arc<Mutex<Vec<HeaderMap>>>,
    hits: Arc<AtomicUsize>,
}

async fn anthropic_messages(State(seen): State<Seen>, headers: HeaderMap, Json(body): Json<Value>) -> Json<Value> {
    seen.hits.fetch_add(1, Ordering::SeqCst);
    seen.headers.lock().expect("not poisoned").push(headers);
    seen.bodies.lock().expect("not poisoned").push(body);
    Json(json!({
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet-5-20260901",
        "content": [{ "type": "text", "text": "hello from the fake" }],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": { "input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": 3, "cache_creation_input_tokens": 0 }
    }))
}

async fn openai_chat_completions(State(seen): State<Seen>, headers: HeaderMap, Json(body): Json<Value>) -> Json<Value> {
    seen.hits.fetch_add(1, Ordering::SeqCst);
    seen.headers.lock().expect("not poisoned").push(headers);
    let wants_tool = body["messages"].to_string().contains("call a tool");
    seen.bodies.lock().expect("not poisoned").push(body);
    let message = if wants_tool {
        json!({ "role": "assistant", "content": null, "tool_calls": [{ "id": "call_1", "type": "function", "function": { "name": "search_kb", "arguments": "{\"q\":\"refunds\"}" } }] })
    } else {
        json!({ "role": "assistant", "content": "hello from the fake" })
    };
    Json(json!({
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "created": 1,
        "model": "gpt-fake-1",
        "choices": [{ "index": 0, "message": message, "finish_reason": if wants_tool { "tool_calls" } else { "stop" } }],
        "usage": { "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
                   "prompt_tokens_details": { "cached_tokens": 3 }, "completion_tokens_details": { "reasoning_tokens": 2 } }
    }))
}

async fn serve(seen: Seen) -> SocketAddr {
    let app = Router::new()
        .route("/v1/messages", post(anthropic_messages))
        .route("/v1/chat/completions", post(openai_chat_completions))
        .with_state(seen);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("binds");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serves") });
    addr
}

fn profile(kind: AgentModelProviderKind, model: &str, base_url: Option<String>) -> AgentModelProfile {
    AgentModelProfile {
        profile_id: AgentModelProfileId::new("fake").expect("id"),
        revision: AgentRevisionNumber::INITIAL,
        provider: kind,
        model: model.to_string(),
        base_url,
        credential_binding: None,
        default_sampling: AgentSamplingSettings::default(),
        capabilities: AgentModelCapabilities { tool_calls: true },
        attributes: BTreeMap::new(),
    }
}

fn request(text: &str) -> AgentModelRequest {
    let descriptor = AgentToolDescriptor::new(
        AgentToolId::new("search_kb").expect("tool id"),
        AgentToolKind::Function,
        "Searches the knowledge base.",
        AgentSchemaRef::new(AgentSchemaId::new("kb-input").expect("schema"), AgentRevisionNumber::INITIAL),
        AgentSchemaRef::new(AgentSchemaId::new("kb-output").expect("schema"), AgentRevisionNumber::INITIAL),
    )
    .expect("descriptor");
    AgentModelRequest::new(AgentContextSnapshotRef::new(text).expect("ref"), 1).with_tools(vec![descriptor])
}

#[tokio::test]
async fn the_anthropic_round_trip_carries_the_key_the_tools_and_the_provenance() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let adapter = RigProviderAdapter::new(
        profile(AgentModelProviderKind::Anthropic, "claude-sonnet-5", Some(format!("http://{addr}"))),
        reqwest::Client::new(),
    )
    .expect("valid");
    let credential = AgentEphemeralCredential::api_key("x-api-key", "sk-ant-fake-sentinel");

    let turn = adapter.call_with(&request("say hello"), Some(&credential)).await.expect("answers");
    assert_eq!(turn.text.as_deref(), Some("hello from the fake"));
    assert_eq!(turn.response_model.as_deref(), Some("claude-sonnet-5-20260901"));
    assert_eq!(turn.finish_reason.as_deref(), Some("end_turn"));
    assert_eq!(turn.usage.input_tokens, 13, "cached tokens are billed as input, as before");
    assert_eq!(turn.usage.cached_input_tokens, Some(3));

    let headers = seen.headers.lock().expect("not poisoned");
    assert_eq!(headers[0].get("x-api-key").and_then(|v| v.to_str().ok()), Some("sk-ant-fake-sentinel"));
    let body = &seen.bodies.lock().expect("not poisoned")[0];
    let tools: Vec<&str> = body["tools"].as_array().expect("tools").iter().filter_map(|t| t["name"].as_str()).collect();
    assert!(tools.contains(&"search_kb") && tools.contains(&"submit_result"), "{tools:?}");
    assert_eq!(body["model"], "claude-sonnet-5");
}

#[tokio::test]
async fn the_openai_completions_round_trip_carries_the_bearer_and_maps_a_tool_call() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let adapter = RigProviderAdapter::new(
        profile(AgentModelProviderKind::OpenAiCompletions, "gpt-fake-1", Some(format!("http://{addr}/v1"))),
        reqwest::Client::new(),
    )
    .expect("valid");
    let credential = AgentEphemeralCredential::bearer_token("sk-openai-fake-sentinel");

    let turn = adapter.call_with(&request("please call a tool"), Some(&credential)).await.expect("answers");
    assert_eq!(turn.tool_calls.len(), 1);
    assert_eq!(turn.tool_calls[0].tool.as_str(), "search_kb");
    assert_eq!(turn.tool_calls[0].arguments["q"], "refunds");
    assert_eq!(turn.response_model.as_deref(), Some("gpt-fake-1"));
    assert_eq!(turn.finish_reason.as_deref(), Some("tool_calls"));
    assert_eq!(turn.usage.cached_input_tokens, Some(3));
    assert_eq!(turn.usage.reasoning_tokens, Some(2));
    let headers = seen.headers.lock().expect("not poisoned");
    assert_eq!(
        headers[0].get("authorization").and_then(|v| v.to_str().ok()),
        Some("Bearer sk-openai-fake-sentinel")
    );
}

#[tokio::test]
async fn a_custom_provider_is_the_completions_shape_at_the_profiles_base_url() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let adapter = RigProviderAdapter::new(
        profile(AgentModelProviderKind::Custom("gateway".to_string()), "llama-3", Some(format!("http://{addr}/v1"))),
        reqwest::Client::new(),
    )
    .expect("valid");
    let turn = adapter
        .call_with(&request("say hello"), Some(&AgentEphemeralCredential::bearer_token("t")))
        .await
        .expect("answers");
    assert_eq!(turn.text.as_deref(), Some("hello from the fake"));
    assert_eq!(seen.hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn an_invalid_or_missing_base_url_is_refused_before_any_request() {
    let error = RigProviderAdapter::new(
        profile(AgentModelProviderKind::Anthropic, "m", Some("https://u:p@host/v1".to_string())),
        reqwest::Client::new(),
    )
    .expect_err("refused at construction");
    assert_eq!(error.code(), "model-profile-invalid-base-url");
    let error = RigProviderAdapter::new(profile(AgentModelProviderKind::Custom("x".to_string()), "m", None), reqwest::Client::new())
        .expect_err("a custom provider needs a base url");
    assert_eq!(error.code(), "model-profile-invalid-base-url");
}

#[tokio::test]
async fn unsupported_or_missing_credential_material_is_refused_before_any_request() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let adapter = RigProviderAdapter::new(
        profile(AgentModelProviderKind::Anthropic, "m", Some(format!("http://{addr}"))),
        reqwest::Client::new(),
    )
    .expect("valid");
    let basic = AgentEphemeralCredential::basic("user", "pw");
    let error = adapter.call_with(&request("x"), Some(&basic)).await.expect_err("basic is not a provider key");
    assert_eq!(error.code(), "model-credential-material-unsupported");
    let error = adapter.call_with(&request("x"), None).await.expect_err("anthropic needs a key");
    assert_eq!(error.code(), "model-credential-missing");
    assert_eq!(seen.hits.load(Ordering::SeqCst), 0, "nothing reached the fake");
}

/// Every send goes through the injected backend: a counting wrapper around
/// `reqwest::Client` that implements rig's `HttpClientExt` by delegation.
#[derive(Clone, Default, Debug)]
struct Counting {
    inner: reqwest::Client,
    sends: Arc<AtomicUsize>,
}

#[tokio::test]
async fn every_send_goes_through_the_injected_backend() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let counting = Counting::default();
    let adapter = RigProviderAdapter::new(
        profile(AgentModelProviderKind::OpenAiCompletions, "gpt-fake-1", Some(format!("http://{addr}/v1"))),
        counting.clone(),
    )
    .expect("valid");
    adapter
        .call_with(&request("say hello"), Some(&AgentEphemeralCredential::bearer_token("t")))
        .await
        .expect("answers");
    assert_eq!(counting.sends.load(Ordering::SeqCst), 1);
    assert_eq!(seen.hits.load(Ordering::SeqCst), 1);
}
```

Implement `HttpClientExt for Counting` by reading `impl HttpClientExt for reqwest::Client` in `rig-core-0.37.0/src/http_client/mod.rs:137` and delegating every required method to `self.inner`, incrementing `sends` in the one that performs a request (name the methods exactly as the trait declares them; the host's `EgressHttpClient` in `../paloul.rakka.host/crates/rakka-host-runtime/src/providers.rs` is the precedent shape and may be read for the method list). If the trait cannot be implemented in an integration test because of a private supertrait, move `Counting` into `rig.rs`'s test module and keep the test there; say so in the report.

The fake's JSON bodies must decode through rig's own provider response types (`providers/anthropic/completion.rs:56`, `providers/openai/completion/mod.rs:898`); adjust field names only if a decode error names one, and record the adjustment.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p rakka-agent --test rig_provider_fake_endpoint`
Expected: compile error, `RigProviderAdapter` does not exist (and, before the dev-dependencies are added, `axum`/`reqwest` unresolved).

- [ ] **Step 3: The dev-dependencies**

Add to `crates/rakka-agent/Cargo.toml` `[dev-dependencies]`:

```toml
# The in-process fake providers `rig_provider_fake_endpoint.rs` serves, and
# the plain-HTTP backend it injects into the provider adapter. Dev-only: the
# publishable tree keeps rig's own reqwest, with `rustls` alone.
axum.workspace = true
reqwest = { version = "0.13", default-features = false }
```

- [ ] **Step 4: The provider adapter**

Add to `rig.rs`, after `impl<M> AgentModelAdapter for RigModelAdapter<M>`:

```rust
/// A per-profile Rig adapter that builds the provider client inside each
/// attempt from the credential the dispatcher resolved, over an HTTP backend
/// injected at construction.
///
/// The adapter never constructs an HTTP backend of its own: `reqwest::Client`
/// is the plain case, and a deployment with an egress policy injects its own
/// [`HttpClientExt`] type — every provider call leaves through the backend the
/// deployment chose. Per attempt, `call_with` validates the profile's base
/// URL, turns the ephemeral credential into the provider's key, builds the
/// provider client over the injected backend, delegates the request to a
/// [`RigModelAdapter`] over that client's completion model, and drops the
/// client with the credential. Nothing long-lived holds a key.
///
/// [`HttpClientExt`]: rig_core::http_client::HttpClientExt
pub struct RigProviderAdapter<H> {
    profile: AgentModelProfile,
    http: H,
    adapter_version: AgentRevisionNumber,
    retry_policy: AgentModelRetryPolicy,
    result_tool: String,
}

impl<H> fmt::Debug for RigProviderAdapter<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RigProviderAdapter")
            .field("profile", &self.profile.profile_id)
            .field("provider", &self.profile.provider)
            .finish_non_exhaustive()
    }
}

impl<H> RigProviderAdapter<H>
where
    H: HttpClientExt + Clone + fmt::Debug + Default + Send + Sync + 'static,
{
    /// An adapter for one validated profile over the given backend.
    ///
    /// # Errors
    ///
    /// The profile's own refusal, and `model-profile-invalid-base-url` for a
    /// `Custom` or `AzureOpenAi` profile with no `base_url`.
    pub fn new(profile: AgentModelProfile, http: H) -> Result<Self, AgentModelProfileError> {
        profile.validate()?;
        if matches!(
            profile.provider,
            AgentModelProviderKind::Custom(_) | AgentModelProviderKind::AzureOpenAi
        ) && profile.base_url.is_none()
        {
            return Err(AgentModelProfileError::InvalidBaseUrl {
                reason: "this provider kind needs a base_url",
            });
        }
        Ok(Self {
            profile,
            http,
            adapter_version: CURRENT_AGENT_LOOP_ADAPTER_VERSION,
            retry_policy: AgentModelRetryPolicy::DEFAULT,
            result_tool: AGENT_RESULT_TOOL_DEFAULT.to_string(),
        })
    }

    /// Stamps a specific adapter version onto the turns this adapter produces.
    #[must_use]
    pub fn with_adapter_version(mut self, adapter_version: AgentRevisionNumber) -> Self {
        self.adapter_version = adapter_version;
        self
    }

    /// Declares the retry policy this adapter's calls dispatch under.
    ///
    /// # Errors
    ///
    /// An invalid policy, as [`RigModelAdapter::with_retry_policy`].
    pub fn with_retry_policy(mut self, policy: AgentModelRetryPolicy) -> AgentModelResult<Self> {
        policy.validate()?;
        self.retry_policy = policy;
        Ok(self)
    }

    /// Names the result tool declared on every request.
    #[must_use]
    pub fn with_result_tool(mut self, result_tool: impl Into<String>) -> Self {
        self.result_tool = result_tool.into();
        self
    }

    /// The provider key an ephemeral credential yields, when the material is a
    /// kind a provider accepts.
    fn provider_key(credential: Option<&AgentEphemeralCredential>) -> AgentModelResult<Option<String>> {
        use rakka_agent_workflow::AgentEphemeralCredentialMaterial as Material;
        match credential.map(AgentEphemeralCredential::material) {
            None => Ok(None),
            Some(Material::BearerToken { token }) => Ok(Some(token.clone())),
            Some(Material::ApiKey { value, .. }) => Ok(Some(value.clone())),
            Some(other) => Err(AgentModelError::Refused {
                code: "model-credential-material-unsupported",
                message: format!(
                    "a provider takes an API key or a bearer token; the resolved credential is {}",
                    other.kind_label()
                ),
            }),
        }
    }

    fn required_key(key: Option<String>, provider: &AgentModelProviderKind) -> AgentModelResult<String> {
        key.ok_or_else(|| AgentModelError::Refused {
            code: "model-credential-missing",
            message: format!(
                "the provider {} needs a credential and the attempt resolved none",
                provider.as_label()
            ),
        })
    }

    fn inner<M: CompletionModel>(&self, model: M, extractor: Option<fn(&M::Response) -> AgentModelResponseMetadata>) -> RigModelAdapter<M> {
        let mut adapter = RigModelAdapter::new(model)
            .with_adapter_version(self.adapter_version)
            .with_result_tool(self.result_tool.clone());
        if let Ok(with_policy) = adapter.clone().with_retry_policy(self.retry_policy) {
            adapter = with_policy;
        }
        if let Some(extractor) = extractor {
            adapter = adapter.with_response_metadata(extractor);
        }
        adapter
    }

    async fn call_provider(
        &self,
        request: &AgentModelRequest,
        credential: Option<&AgentEphemeralCredential>,
    ) -> AgentModelResult<AgentModelTurn> {
        use rig_core::client::CompletionClient as _;
        use rig_core::providers::{anthropic, azure, gemini, ollama, openai, openrouter};

        let key = Self::provider_key(credential)?;
        let profile = &self.profile;
        let http = self.http.clone();
        match &profile.provider {
            AgentModelProviderKind::Anthropic => {
                let mut builder = anthropic::Client::builder()
                    .api_key(Self::required_key(key, &profile.provider)?)
                    .http_client(http);
                if let Some(url) = &profile.base_url {
                    builder = builder.base_url(url);
                }
                if let Some(version) = profile.attributes.get("anthropic_version") {
                    builder = builder.anthropic_version(version);
                }
                let client = builder.build().map_err(provider_error)?;
                self.inner(client.completion_model(&profile.model), Some(anthropic_response_metadata))
                    .call(request)
                    .await
            }
            AgentModelProviderKind::OpenAiResponses => {
                let mut builder = openai::Client::builder()
                    .api_key(Self::required_key(key, &profile.provider)?)
                    .http_client(http);
                if let Some(url) = &profile.base_url {
                    builder = builder.base_url(url);
                }
                let client = builder.build().map_err(provider_error)?;
                self.inner(client.completion_model(&profile.model), None).call(request).await
            }
            AgentModelProviderKind::OpenAiCompletions | AgentModelProviderKind::Custom(_) => {
                let mut builder = openai::Client::builder()
                    .api_key(Self::required_key(key, &profile.provider)?)
                    .http_client(http);
                if let Some(url) = &profile.base_url {
                    builder = builder.base_url(url);
                }
                let client = builder.build().map_err(provider_error)?.completions_api();
                self.inner(
                    client.completion_model(&profile.model),
                    Some(openai_completions_response_metadata),
                )
                .call(request)
                .await
            }
            AgentModelProviderKind::OpenRouter => {
                let mut builder = openrouter::Client::builder()
                    .api_key(Self::required_key(key, &profile.provider)?)
                    .http_client(http);
                if let Some(url) = &profile.base_url {
                    builder = builder.base_url(url);
                }
                let client = builder.build().map_err(provider_error)?;
                self.inner(client.completion_model(&profile.model), None).call(request).await
            }
            AgentModelProviderKind::Gemini => {
                let mut builder = gemini::Client::builder()
                    .api_key(Self::required_key(key, &profile.provider)?)
                    .http_client(http);
                if let Some(url) = &profile.base_url {
                    builder = builder.base_url(url);
                }
                let client = builder.build().map_err(provider_error)?;
                self.inner(client.completion_model(&profile.model), None).call(request).await
            }
            AgentModelProviderKind::AzureOpenAi => {
                let endpoint = profile.base_url.clone().unwrap_or_default();
                let mut builder = azure::Client::builder()
                    .api_key(azure::AzureOpenAIAuth::ApiKey(Self::required_key(key, &profile.provider)?))
                    .http_client(http)
                    .azure_endpoint(endpoint);
                if let Some(version) = profile.attributes.get("api_version") {
                    builder = builder.api_version(version);
                }
                let client = builder.build().map_err(provider_error)?;
                self.inner(client.completion_model(&profile.model), None).call(request).await
            }
            AgentModelProviderKind::Ollama => {
                let mut builder = ollama::Client::builder()
                    .api_key(ollama::OllamaApiKey::from(key.unwrap_or_default()))
                    .http_client(http);
                if let Some(url) = &profile.base_url {
                    builder = builder.base_url(url);
                }
                let client = builder.build().map_err(provider_error)?;
                self.inner(client.completion_model(&profile.model), None).call(request).await
            }
        }
    }
}

impl<H> AgentModelAdapter for RigProviderAdapter<H>
where
    H: HttpClientExt + Clone + fmt::Debug + Default + Send + Sync + 'static,
{
    fn adapter_version(&self) -> AgentRevisionNumber {
        self.adapter_version
    }

    fn retry_policy(&self) -> AgentModelRetryPolicy {
        self.retry_policy
    }

    fn call<'a>(&'a self, request: &'a AgentModelRequest) -> AgentModelFuture<'a> {
        self.call_with(request, None)
    }

    fn call_with<'a>(
        &'a self,
        request: &'a AgentModelRequest,
        credential: Option<&'a AgentEphemeralCredential>,
    ) -> AgentModelFuture<'a> {
        Box::pin(async move { self.call_provider(request, credential).await })
    }
}
```

Notes for the implementer: `provider_error` already maps rig errors in this module; the `http_client` order (`api_key` first, then `http_client`) follows rig's builder typestate (`client/mod.rs:594–603`, `:658`); the exact paths of `AzureOpenAIAuth`, `OllamaApiKey`, and each `Client` alias are the ones the verification list at the top of this plan names; if a provider's `build()` requires an extra bound on `H` that `reqwest::Client` satisfies, add it to the adapter's `where` clause and record it. If `RigModelAdapter` is not `Clone` for the retry-policy hop in `inner`, apply the policy before the other builders. Nothing in `RigModelAdapter` changes.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p rakka-agent --test rig_provider_fake_endpoint` then `cargo test -p rakka-agent`, `cargo check -p rakka-agent --no-default-features`, `cargo clippy -p rakka-agent --all-targets --all-features -- -D warnings`, `cargo fmt --all -- --check`.
Expected: the six fake-endpoint tests pass; everything else unchanged.

- [ ] **Step 6: Commit**

```bash
git add crates/rakka-agent/Cargo.toml crates/rakka-agent/src/rig.rs crates/rakka-agent/tests/rig_provider_fake_endpoint.rs
git commit -m "Reach every Rig provider through a per-attempt client built over an injected backend from the resolved credential

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: The example's gated provider walk over an env-backed credential resolver

**Files:**
- Modify: `examples/durable-agent-acceptance/Cargo.toml` (`[dependencies]`: `reqwest = { version = "0.13", default-features = false, features = ["rustls"] }`)
- Modify: `examples/durable-agent-acceptance/src/wiring.rs` (`World` at 147–200: a `model: Arc<dyn AgentModelAdapter>` field beside `adapter`, optional `profiles` and `credentials` fields, `World::with_model`; `pipeline()` at 337 uses them)
- Create: `examples/durable-agent-acceptance/src/provider.rs` (`EnvCredentialResolver`, `provider_profile_from_env`, `run_provider_walk`)
- Modify: `examples/durable-agent-acceptance/src/lib.rs` (`pub mod provider;`, re-export `run_provider_walk`, `ProviderWalkReport`)
- Modify: `examples/durable-agent-acceptance/src/main.rs` (`--provider` flag)
- Create: `examples/durable-agent-acceptance/tests/provider_walk.rs` (gated)
- Modify: `examples/durable-agent-acceptance/README.md` (a "Live provider walk (gated)" section), `CLAUDE.md` (one line in "Gated / optional tests")

**Interfaces:**
- Consumes: Task 1's `AgentModelProfile`, `AgentModelProviderKind`, `StaticAgentModelProfileCatalog`; Task 2's `AgentToolAuthority::with_model_profiles`; Task 6's `RigProviderAdapter<reqwest::Client>`; the dispatcher's `with_credential_resolver(Arc<dyn AgentEffectCredentialResolver>)` (`dispatch.rs:2085`) and the trait at `:1348`; `AgentSettings.model_profile` (`definition.rs:1183`) narrowed against `AgentAuthorityEnvelope.model_profiles` (`:649`); `AgentAuthorityEnvelope.credential_bindings` (`definition.rs:660`); `AgentDispatchFuture` and `AgentDispatchError::collaborator(code, message)` (re-exported at the crate root, `lib.rs:151`; the testkit's `ScriptedCredentialResolver` at `testkit.rs:3402` is the precedent body); `AgentEffectPolicies::with_model_spec` (`effect.rs:776`, fallible) and `AgentEffectSpec::with_timeout_ms` (`:431`), the only writer of a model intent's `timeout_ms` (`effect.rs:1895` copies `spec.timeout_ms`).
- Produces:
  - `World::with_model(model: Arc<dyn AgentModelAdapter>, profiles: Option<Arc<dyn AgentModelProfileCatalog>>, credentials: Option<Arc<dyn AgentEffectCredentialResolver>>, catalog_target: A2AAgentTarget) -> Self`; `World::new` keeps its signature and delegates with `Arc::new(adapter.clone())`, `None`, `None`.
  - `pub struct EnvCredentialResolver { var: String }` implementing `AgentEffectCredentialResolver` by reading the named environment variable at resolve time into `AgentEphemeralCredential::bearer_token` or `::api_key("x-api-key", …)` depending on the provider kind; the variable's value is never stored on the struct.
  - `pub struct ProviderWalkReport { pub lines: Vec<String>, pub response_model: Option<String>, pub tool_invocations: usize }` and `pub async fn run_provider_walk() -> Result<ProviderWalkReport, String>` (Err when the gate variables are unset, naming the variable).
  - Gate: `RAKKA_MODEL_PROFILE` (the profile id, e.g. `live`), `RAKKA_MODEL_PROVIDER` (`anthropic` | `openai-completions` | `openai-responses` | `openrouter` | `gemini` | `ollama` | `custom`), `RAKKA_MODEL_NAME`, optional `RAKKA_MODEL_BASE_URL`, and `RAKKA_MODEL_API_KEY` (the credential; read by the resolver only, never by the walk). Ollama with no key is allowed.

**Ruling on spec 4.7's "runs the same walk".** The 18-line acceptance transcript is scripted around content sentinels, a deterministic two-turn flow, and a dispatcher-death bullet; a live model cannot reproduce it. The gated walk therefore reuses the world (real sharding, dispatcher fleet, A2A core, in-memory stores) and the same task definition, tool, and envelope, but asserts structural facts instead of the transcript: the run reaches a terminal status, at least one model turn was recorded with the adapter's `response_model` filled, the recording tool executor saw at least one invocation when the model chose to call it (reported, not required), and no byte of `RAKKA_MODEL_API_KEY` appears in any durable store, the fleet index, or the transcript. Recorded in the spec's section 4.7 as a plan refinement.

- [ ] **Step 1: Write the failing test**

Create `examples/durable-agent-acceptance/tests/provider_walk.rs`:

```rust
//! The live-provider walk, gated: with no `RAKKA_MODEL_PROFILE` this test
//! announces the skip and passes; with the gate set it drives the world's
//! run through the real Rig provider adapter and asserts the structural
//! facts a live model can be held to.

use rakka_example_durable_agent_acceptance::run_provider_walk;

#[tokio::test]
async fn the_provider_walk_is_gated_and_holds_its_facts_when_armed() {
    match run_provider_walk().await {
        Err(reason) => {
            assert!(reason.contains("RAKKA_MODEL_PROFILE"), "the skip names its gate: {reason}");
            eprintln!("skipped: {reason}");
        }
        Ok(report) => {
            assert!(report.lines.iter().any(|line| line.starts_with("ok  terminal:")), "{:?}", report.lines);
            assert!(report.response_model.is_some(), "a live provider reports the model that answered");
            let key = std::env::var("RAKKA_MODEL_API_KEY").unwrap_or_default();
            if !key.is_empty() {
                for line in &report.lines {
                    assert!(!line.contains(&key), "the key never reaches the transcript");
                }
            }
        }
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p rakka-example-durable-agent-acceptance --test provider_walk`
Expected: compile error, `run_provider_walk` does not exist.

- [ ] **Step 3: The world takes any adapter**

In `wiring.rs`, add to `World`:

```rust
    /// The adapter the dispatcher fleet invokes: the deterministic one in
    /// the acceptance walk, a live provider adapter in the gated walk.
    pub model: Arc<dyn rakka_agent::AgentModelAdapter>,
    /// The model profile catalog the authority resolves, when the walk has one.
    pub profiles: Option<Arc<dyn rakka_agent::AgentModelProfileCatalog>>,
    /// The credential resolver the dispatcher consults, when the walk has one.
    pub credentials: Option<Arc<dyn rakka_agent::AgentEffectCredentialResolver>>,
    /// A timeout for model effects, when the walk needs one: a credential-bearing
    /// model call with no `timeout_ms` is refused `model-timeout-unset`.
    pub model_timeout_ms: Option<u64>,
```

`with_model` initialises `model_timeout_ms: None`; the provider walk sets it after construction. In `pipeline()`, where the delivery's policies come from `self.registry.effect_policies()`, apply the timeout when present:

```rust
                .with_effect_policies({
                    let policies = self.registry.effect_policies().expect("the registry projects valid policies");
                    match self.model_timeout_ms {
                        Some(timeout_ms) => policies
                            .with_model_spec(rakka_agent::AgentEffectSpec::read_only().with_timeout_ms(timeout_ms))
                            .expect("a read-only model spec with a timeout is valid"),
                        None => policies,
                    }
                })
```

(`AgentEffectPolicies::new()` already gives the model spec `read_only()`; only the timeout is added.) Make `agent_id` and `agent_scope` in `flow.rs` `pub(crate)` so `provider.rs` can name the same agent.

Rename the current `new` body into `with_model` with the signature above (the deterministic `adapter` field stays, filled with `DeterministicModelAdapter::new()` by `with_model` and with the scripted adapter by `new`, because the acceptance flow reads `world.adapter` for its turn facts), and make `new`:

```rust
    pub fn new(adapter: DeterministicModelAdapter, catalog_target: A2AAgentTarget) -> Self {
        let mut world = Self::with_model(Arc::new(adapter.clone()), None, None, catalog_target);
        world.adapter = adapter;
        world
    }
```

In `pipeline()`, replace `Arc::new(self.adapter.clone())` with `self.model.clone()`, build the tool authority as:

```rust
            Arc::new(AgentEntityAuthority::new(self.agents.clone(), {
                let authority = AgentToolAuthority::new(self.registry.clone());
                match &self.profiles {
                    Some(catalog) => authority.with_model_profiles(catalog.clone()),
                    None => authority,
                }
            })),
```

and after the dispatcher is constructed, apply the resolver when present (`let dispatcher = …; match &self.credentials { Some(resolver) => dispatcher.with_credential_resolver(resolver.clone()), None => dispatcher }`), keeping the `Pipeline` type alias as is. If the acceptance flow constructs `World` fields by literal anywhere else, add the four fields there.

- [ ] **Step 4: The resolver and the walk**

Create `examples/durable-agent-acceptance/src/provider.rs`:

```rust
//! The gated live-provider walk: the same world, task, tool, and envelope as
//! the acceptance walk, driven through the real Rig provider adapter over an
//! env-backed credential resolver that lives here, in the example, and never
//! in a crate.

use std::collections::BTreeMap;
use std::sync::Arc;

use rakka_agent::{
    AgentCredentialBindingRef, AgentDispatchError, AgentDispatchFuture,
    AgentEffectCredentialResolver, AgentModelCapabilities, AgentModelProfile,
    AgentModelProfileId, AgentModelProviderKind, AgentRevisionNumber, AgentRunEffect,
    AgentRunScope, AgentSamplingSettings, StaticAgentModelProfileCatalog,
};
use rakka_agent_workflow::AgentEphemeralCredential;
use rakka_a2a::A2AAgentTarget;
use rakka_agent::rig::RigProviderAdapter;

use crate::wiring::World;

/// Resolves one logical binding from one environment variable, read at
/// resolve time. The value lives only in the `AgentEphemeralCredential` the
/// dispatcher drops with the attempt.
pub struct EnvCredentialResolver {
    var: String,
    kind: AgentModelProviderKind,
}

impl EnvCredentialResolver {
    /// A resolver that reads `var` for the given provider kind.
    #[must_use]
    pub fn new(var: impl Into<String>, kind: AgentModelProviderKind) -> Self {
        Self { var: var.into(), kind }
    }
}

impl AgentEffectCredentialResolver for EnvCredentialResolver {
    fn resolve<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _binding: &'a AgentCredentialBindingRef,
        _effect: &'a AgentRunEffect,
    ) -> AgentDispatchFuture<'a, AgentEphemeralCredential> {
        Box::pin(async move {
            let value = std::env::var(&self.var).map_err(|_| {
                AgentDispatchError::collaborator(
                    "credential-env-unset".to_string(),
                    format!("{} is not set", self.var),
                )
            })?;
            Ok(match self.kind {
                AgentModelProviderKind::Anthropic => AgentEphemeralCredential::api_key("x-api-key", value),
                _ => AgentEphemeralCredential::bearer_token(value),
            })
        })
    }
}
```

(`AgentDispatchError::collaborator` takes the code and message as the testkit's resolver passes them at `testkit.rs:3402`; match its argument types.) Then the walk:

```rust
/// What the gated walk reports.
#[derive(Debug)]
pub struct ProviderWalkReport {
    /// One line per fact, `ok  …` when it held.
    pub lines: Vec<String>,
    /// The response model the provider reported on the first recorded turn.
    pub response_model: Option<String>,
    /// How many times the recording tool executor was invoked.
    pub tool_invocations: usize,
}

fn provider_kind(name: &str) -> Result<AgentModelProviderKind, String> {
    Ok(match name {
        "anthropic" => AgentModelProviderKind::Anthropic,
        "openai-completions" => AgentModelProviderKind::OpenAiCompletions,
        "openai-responses" => AgentModelProviderKind::OpenAiResponses,
        "openrouter" => AgentModelProviderKind::OpenRouter,
        "gemini" => AgentModelProviderKind::Gemini,
        "ollama" => AgentModelProviderKind::Ollama,
        "custom" => AgentModelProviderKind::Custom("custom".to_string()),
        other => return Err(format!("RAKKA_MODEL_PROVIDER={other} is not a known provider kind")),
    })
}

/// The profile the environment describes, or the gate variable that is unset.
pub fn provider_profile_from_env() -> Result<AgentModelProfile, String> {
    let profile_id = std::env::var("RAKKA_MODEL_PROFILE").map_err(|_| "RAKKA_MODEL_PROFILE is unset; the live provider walk is skipped".to_string())?;
    let provider = provider_kind(&std::env::var("RAKKA_MODEL_PROVIDER").map_err(|_| "RAKKA_MODEL_PROVIDER is unset".to_string())?)?;
    let model = std::env::var("RAKKA_MODEL_NAME").map_err(|_| "RAKKA_MODEL_NAME is unset".to_string())?;
    let binding = (!matches!(provider, AgentModelProviderKind::Ollama) || std::env::var("RAKKA_MODEL_API_KEY").is_ok())
        .then(|| AgentCredentialBindingRef::new("live-provider-key").expect("the binding ref is valid"));
    let profile = AgentModelProfile {
        profile_id: AgentModelProfileId::new(&profile_id).map_err(|error| format!("RAKKA_MODEL_PROFILE: {error}"))?,
        revision: AgentRevisionNumber::INITIAL,
        provider,
        model,
        base_url: std::env::var("RAKKA_MODEL_BASE_URL").ok(),
        credential_binding: binding,
        default_sampling: AgentSamplingSettings::default(),
        capabilities: AgentModelCapabilities { tool_calls: true },
        attributes: BTreeMap::new(),
    };
    profile.validate().map_err(|error| format!("the profile from the environment is invalid: {error}"))?;
    Ok(profile)
}

/// Runs the world's run through the live provider and reports the facts.
///
/// # Errors
///
/// The gate variable that is unset, or an invalid profile.
pub async fn run_provider_walk() -> Result<ProviderWalkReport, String> {
    let profile = provider_profile_from_env()?;
    let adapter = RigProviderAdapter::new(profile.clone(), reqwest::Client::new())
        .map_err(|error| format!("the provider adapter refused the profile: {error}"))?;
    let catalog = StaticAgentModelProfileCatalog::new().with_profile(profile.clone());
    let credentials: Option<Arc<dyn AgentEffectCredentialResolver>> = profile
        .credential_binding
        .is_some()
        .then(|| Arc::new(EnvCredentialResolver::new("RAKKA_MODEL_API_KEY", profile.provider.clone())) as Arc<_>);
    let mut world = World::with_model(
        Arc::new(adapter),
        Some(Arc::new(catalog)),
        credentials,
        A2AAgentTarget::new(crate::flow::agent_id(), crate::flow::task_definition()),
    );
    world.model_timeout_ms = Some(60_000);
    crate::flow::drive_one_run_with_profile(&world, &profile).await
}
```

`drive_one_run_with_profile` is the piece of `flow.rs` to extract: the instantiate → send-task → drive-dispatcher → settle sequence from `run_acceptance` lines 1/18 through the run's terminal bullet, parameterised so the definition envelope also inserts `profile.profile_id` into `envelope.model_profiles` and `profile.credential_binding` into `envelope.credential_bindings`, the settings carry `model_profile: Some(profile.profile_id.clone())`, and the walk keeps driving `pipeline().run_once()`-style passes until the run is terminal or 20 passes elapse. It records `response_model` from the first turn in session memory (`world.session`) or the run's telemetry segments, `tool_invocations` from `world.tools`, and the lines:

```
ok  profile: <id> via <provider kind label>
ok  terminal: <status> after <n> passes
ok  response model: <model or "unreported">
ok  tool invocations: <n>
ok  secret exclusion: <n> durable records scanned, none carries the key
```

The secret-exclusion line scans every store the world holds (`tasks`, `agents`, `runs`, `history`, `workflow_store`, `fleet_store`, `session`, `snapshots`) by serialising each record to JSON and asserting `RAKKA_MODEL_API_KEY`'s value (when set and non-empty) is absent; `crate::flow` already has the sentinel scan for `CONTENT_SENTINELS` — generalise it to take the needle. Keep `run_acceptance` byte-for-byte in its transcript: the extraction is a refactor, and `tests/acceptance.rs` proves it.

`lib.rs`: add `pub mod provider;` and `pub use provider::{run_provider_walk, ProviderWalkReport};`. `main.rs`:

```rust
#[tokio::main]
async fn main() {
    if std::env::args().any(|arg| arg == "--provider") {
        match rakka_example_durable_agent_acceptance::run_provider_walk().await {
            Ok(report) => report.lines.iter().for_each(|line| println!("{line}")),
            Err(reason) => {
                eprintln!("{reason}");
                std::process::exit(2);
            }
        }
        return;
    }
    let report = rakka_example_durable_agent_acceptance::run_acceptance().await;
    for line in &report.lines {
        println!("{line}");
    }
}
```

`Cargo.toml` of the example: add `reqwest = { version = "0.13", default-features = false, features = ["rustls"] }` (rig's `rustls` feature is `reqwest/rustls`; the example names it itself so the HTTPS client does not depend on feature unification).

- [ ] **Step 5: The docs**

README, after "## Durable-write budget" (or at the end):

````markdown
## Live provider walk (gated)

The same world, task, tool, and envelope, driven through the real Rig
provider adapter over an env-backed credential resolver that lives in this
example. Skipped unless `RAKKA_MODEL_PROFILE` is set:

```sh
RAKKA_MODEL_PROFILE=live RAKKA_MODEL_PROVIDER=anthropic RAKKA_MODEL_NAME=claude-sonnet-5 \
RAKKA_MODEL_API_KEY=... cargo run -p rakka-example-durable-agent-acceptance -- --provider
RAKKA_MODEL_PROFILE=live RAKKA_MODEL_PROVIDER=openai-completions RAKKA_MODEL_NAME=gpt-5 \
RAKKA_MODEL_API_KEY=... cargo test -p rakka-example-durable-agent-acceptance --test provider_walk -- --nocapture
```

`RAKKA_MODEL_PROVIDER` is one of `anthropic`, `openai-completions`,
`openai-responses`, `openrouter`, `gemini`, `ollama`, `custom`;
`RAKKA_MODEL_BASE_URL` overrides the provider's endpoint (required for
`custom`); `ollama` needs no key. The walk asserts structure, not a
transcript: the run terminates, the provider's response model is recorded on
the turn, and the key appears in no durable record and no line of output.
````

CLAUDE.md "Gated / optional tests" block, one line after the OTLP collector line:

```sh
RAKKA_MODEL_PROFILE=live RAKKA_MODEL_PROVIDER=anthropic RAKKA_MODEL_NAME=claude-sonnet-5 RAKKA_MODEL_API_KEY=... cargo test -p rakka-example-durable-agent-acceptance --test provider_walk -- --nocapture   # live model provider walk
```

- [ ] **Step 6: Run the tests**

Run: `cargo test -p rakka-example-durable-agent-acceptance` (both test files; the provider one announces its skip) then `cargo run -p rakka-example-durable-agent-acceptance` (transcript unchanged) and `cargo run -p rakka-example-durable-agent-acceptance -- --provider` (exit 2 with the skip reason), `cargo clippy -p rakka-example-durable-agent-acceptance --all-targets -- -D warnings`, `cargo fmt --all -- --check`. Do not run the armed walk in this task: there is no key on this machine, and the plan does not ask for one. If `RAKKA_MODEL_PROVIDER=ollama RAKKA_MODEL_PROFILE=live RAKKA_MODEL_NAME=<a pulled model> RAKKA_MODEL_BASE_URL=http://127.0.0.1:11434` happens to be serviceable locally, running it once is welcome and its output goes in the report; its absence is not a failure.
Expected: all pass; the acceptance transcript is unchanged.

- [ ] **Step 7: Commit**

```bash
git add examples/durable-agent-acceptance CLAUDE.md
git commit -m "Add the gated live-provider walk to the durable agent acceptance example over an env-backed credential resolver

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: Documentation and the compatibility record

**Files:**
- Modify: `docs/plans/rakka-agent/spec.md:1076-1094` (section 10.1: the `call_with` sentence)
- Modify: `docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md` (the 2026-09-21 refinement marks for 4.2, 4.4, 4.6, 4.7, 11.1 and the section 0 note are already in the tree, uncommitted — verify they read correctly and commit them here)
- Modify: `docs/rakka-agents.md:121-123` (the loop's model-adapter sentence)
- Modify: `docs/rakka-compatibility.md` (a new Phase 7 slice 7.1 bullet after the slice 7.2 bullet at line 91; the `rig-core` row of the pin table at line 165)
- Modify: `CHANGELOG.md` (Unreleased → Added, one `rakka-agent` bullet)
- Modify: `docs/rakka-agent-telemetry-validation-matrix.md:46` (the 17.8 row) and `:238-241` (the owed bullet)
- Modify: `docs/rakka-agent-security-validation-matrix.md:112` (the descriptor row) and `:167-169` (the owed bullet)

**Interfaces:**
- Consumes: every name Tasks 1–7 produced. Nothing produced.

- [ ] **Step 1: The doc-holding tests**

Run: `cargo test -p rakka-agent --test compatibility_currency --test crate_shape --test schema_compatibility --features otel` and `cargo test -p rakka-testkit --test docs_currency 2>/dev/null || true` (whichever doc-parsing tests exist; `grep -rln "rakka-compatibility.md\|telemetry-validation-matrix\|security-validation-matrix" crates/*/tests` lists them) before editing, to know the baseline, and after editing, to see what the edits must keep true (the pin table row is parsed: keep its column count and the `=0.37.0` cell).

- [ ] **Step 2: The edits**

`docs/plans/rakka-agent/spec.md` 10.1, after "The durable loop, effect model, and testkit MUST depend only on this trait.": add

```
The trait's `call_with(request, credential)` is the per-attempt entry the
dispatcher uses: the ephemeral credential it resolved for the model
profile's binding is handed to the adapter for that attempt only and is
dropped with it; `call(request)` remains for adapters that need none, and
`call_with` defaults to it. (Phase 7 slice 7.1, 2026-09-21.)
```

`docs/rakka-agents.md` item 3, the parenthetical "(Rig behind the `rig` feature; a deterministic adapter in the testkit)": extend to "(Rig behind the `rig` feature — `RigModelAdapter` over any Rig completion model, and `RigProviderAdapter` over a model profile and an injected HTTP backend, building the provider client per attempt from the credential the dispatcher resolved; a deterministic adapter in the testkit)".

`docs/rakka-compatibility.md`, after the slice 7.2 bullet (line 91), one bullet:

```
- Phase 7 slice 7.1 ("wired model providers") adds a model profile record and catalog, one defaulted trait method, six serde-defaulted fields, and eight stable refusal codes, and changes one Cargo feature's dependency set. `AgentModelAdapter::call_with(request, credential)` is a new **defaulted** method (it calls `call`), so existing adapters compile unchanged; the dispatcher now calls it, handing the ephemeral credential it resolved for the grant's binding to the adapter for that attempt only. `AgentToolAuthority::with_model_profiles(catalog)` installs an `AgentModelProfileCatalog`; with one installed, a model intent whose profile the catalog does not project refuses `model-profile-unknown`, the grant records the profile's `revision` and `digest()` (`AgentDispatchGrant.model_profile_revision`/`model_profile_digest`, serde-defaulted, no schema version moves) and carries the profile's `credential_binding` when the intent names none; a credential-bearing model intent with no `timeout_ms` refuses `model-timeout-unset` before the resolver is consulted, and the intent the resolver receives carries `deadline_at = attempt start + timeout_ms`, recomputed per attempt and never persisted. `AgentGrantedDispatch.tools` and `AgentModelRequest.tools` carry the model-visible descriptors the authority computed from `AgentToolRegistry::model_visible(envelope, settings)`; the Rig adapter declares them on the completion request beside the result tool, so a tool the setup does not expose is never offered to the model. `AgentModelUsage` gains `cached_input_tokens`/`reasoning_tokens` and `AgentModelTurn` gains `response_model`/`finish_reason` (all `Option`, serde-defaulted, absent when unreported; the two strings bounded at `AGENT_MODEL_RESPONSE_FIELD_MAX_BYTES` = 128 bytes, refused `model-response-metadata-too-long` above it, and immutable under a `ModelResponse` transform as `guardrail-transform-invalid`); `AgentTelemetrySegment.model_response` and the span attributes `gen_ai.response.model`, `gen_ai.response.finish_reasons`, `rakka.agent.model.cached_input_tokens`, `rakka.agent.model.reasoning_tokens` join the allowlist. `AgentModelProfile::validate` refuses `model-profile-invalid-base-url` (not `http`/`https`, userinfo, fragment, or query) and `model-profile-invalid-attribute` (a key naming secret material, or an oversized entry); `AgentModelRouter` refuses `model-router-adapter-version-mismatch` at wiring and `model-profile-unknown` at call time; `RigProviderAdapter` refuses `model-credential-missing` and `model-credential-material-unsupported` (`Basic` or `Custom` material) before any request. The adapter and router surface those through the new `AgentModelError::Refused { code, message }` variant, whose `code()` is the stable code. `model-profile-revision-mismatch` is **not** answered in this slice: the checkpoint binding is built from the durable intent alone, so the digest the grant records is not yet compared across attempts. The `rig` feature now declares `rig-core` with `default-features = false, features = ["rustls"]`; previously rig's defaults applied. Rig's `reqwest` feature is never enabled (it turns on `reqwest/system-proxy`, an environment-proxy egress bypass that feature unification would switch on for every crate in a consumer's graph), and `crate_shape.rs` holds the manifest to that; a consumer relying on a rig default feature other than `rustls` must enable it in its own manifest. `RigProviderAdapter<H>` is generic over `rig_core::http_client::HttpClientExt` and constructs no HTTP backend of its own.
```

Pin table `rig-core` row, "Declared in" cell: "`crates/rakka-agent/Cargo.toml`, behind the `rig` feature, with `default-features = false, features = ["rustls"]` (held by `crate_shape.rs`)".

`CHANGELOG.md`, Unreleased → Added, after the last `rakka-agent` bullet:

```
- `rakka-agent` Phase 7 slice 7.1, wired model providers: `AgentModelProfile` (provider kind, model, base URL, logical credential binding, default sampling, capabilities, bounded attributes) with `validate`/`digest`; `AgentModelProfileCatalog` as an on-demand projection with `StaticAgentModelProfileCatalog`; `AgentToolAuthority::with_model_profiles`, which makes the grant carry the profile's revision, digest, and credential binding; `AgentModelAdapter::call_with` (defaulted) with the dispatcher handing the resolved ephemeral credential to the adapter per attempt, stamping `deadline_at` from `timeout_ms` and refusing `model-timeout-unset`; the model-visible tool list on the grant and the request; provider provenance slots on the turn, the usage, the segment, and the span; `AgentModelRouter`; `RigProviderAdapter<H>` over an injected `HttpClientExt` backend for Anthropic, OpenAI (completions and responses), Azure OpenAI, OpenRouter, Gemini, Ollama, and OpenAI-compatible endpoints, proven against in-process fakes; `rig-core` pinned to `rustls` alone; and a gated live-provider walk in `examples/durable-agent-acceptance`.
```

Telemetry matrix row 17.8, last cell: replace "no response model, finish reason, cached/reasoning tokens, streaming timing, snapshot digest, or model-call latency instrument — see "Owed"" with "since Phase 7 slice 7.1 the response model, finish reason, and cached/reasoning tokens have slots (`gen_ai.response.model`, `gen_ai.response.finish_reasons`, `rakka.agent.model.cached_input_tokens`, `rakka.agent.model.reasoning_tokens`), written when the provider reported them; no streaming timing, snapshot digest, or model-call latency instrument — see "Owed"", and add `otel_span_mapping.rs::provider_response_fields_are_written_when_reported_and_omitted_when_not` to the tests cell. The owed bullet becomes "**17.8's streaming and latency fields have no slot.** Streaming flag and first-chunk timing, the context-snapshot digest, and a model-call latency instrument do not exist (response model, finish reason, and cached/reasoning tokens do, since Phase 7 slice 7.1; the request's trace context since gap slice 2); …" keeping the rest.

Security matrix row at line 112, last cell: "**Met, 5 of 5** — the model-visible descriptor rung is wired since Phase 7 slice 7.1: the authority computes `model_visible(envelope, settings)` onto the grant and the Rig adapter declares exactly that list" and add `model_provider_dispatch.rs::the_model_sees_only_the_setup_visible_tools` to the tests cell. Delete the owed bullet "The model-visible descriptor rung." at lines 167–169.

- [ ] **Step 3: Run the doc-holding tests**

Run: the commands from Step 1 again, plus `cargo test -p rakka-agent --test compatibility_currency --features otel` and `cargo doc -p rakka-agent --no-deps --all-features` (`RUSTDOCFLAGS="-D warnings"`).
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add docs/plans/rakka-agent/spec.md docs/superpowers/specs/2026-09-19-phase7-agent-surface-parity-design.md docs/rakka-agents.md docs/rakka-compatibility.md CHANGELOG.md docs/rakka-agent-telemetry-validation-matrix.md docs/rakka-agent-security-validation-matrix.md
git commit -m "Record slice 7.1's model profiles, credential path, provenance slots, and rustls-only Rig pin in the product docs

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Validation and the plan file

**Files:**
- Modify: nothing in `src/`; `docs/superpowers/plans/2026-09-21-phase7-slice-7-1-wired-model-providers.md` is committed here.

- [ ] **Step 1: Format and lint**

Run: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
Expected: clean.

- [ ] **Step 2: Tests, per crate**

The one-shot workspace run is killed on this machine. Run, each to completion:

```sh
cargo test -p rakka-agent --all-features
cargo test -p rakka-agent --no-default-features
cargo test -p rakka-a2a --all-features
cargo test -p rakka-agent-workflow
cargo test -p rakka-example-durable-agent-acceptance
cargo test -p rakka-example-coordination-capability-acceptance
cargo test -p rakka-example-agent-otlp-export-acceptance
cargo test -p rakka
```

Expected: all pass; the provider walk announces its skip.

- [ ] **Step 3: Feature checks and docs**

Run: `cargo check -p rakka-agent --no-default-features`, `cargo check -p rakka-stream --no-default-features`, `cargo check -p rakka-process --no-default-features`, `cargo check -p rakka --no-default-features --features agent`, `cargo check -p rakka --features agent-rig`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features`, and `cargo tree -p rakka-agent -e features -i reqwest | grep -c system-proxy` (expected `0`).
Expected: clean.

- [ ] **Step 4: The canonical validation**

Run: `scripts/validate.sh > /tmp/validate-7-1.log 2>&1; echo "exit=$?"` (redirect, then read the exit code; piping into `tail` reports `tail`'s code). If the workspace test phase is killed, note it and rely on Step 2's per-crate runs, saying so in the report.
Expected: `exit=0`, or the documented kill with every per-crate run green.

- [ ] **Step 5: Commit the plan**

```bash
git add docs/superpowers/plans/2026-09-21-phase7-slice-7-1-wired-model-providers.md
git commit -m "Add the slice 7.1 implementation plan

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

Pushing and opening a pull request wait for the owner's go-ahead; do neither here.

---

## Self-review record

**Spec coverage (section 4 of the Phase 7 design, refined 2026-09-21):**

| Spec item | Task |
| --- | --- |
| 4.1 profile record, `validate`, `digest`, catalog trait as projection, `StaticAgentModelProfileCatalog`, `Custom(String)` | 1 |
| 4.2 item 1 `with_model_profiles`, `model-profile-unknown`, grant revision/digest (mismatch refusal deferred, recorded) | 2 |
| 4.2 item 2 credential path: grant binding from the profile, resolver per attempt, `deadline_at`, `model-timeout-unset` | 2, 3 |
| 4.2 item 3 `AgentModelRequest.tools` from `model_visible` | 2, 3 |
| 4.3 `call_with` defaulted, `AgentModelRouter` | 1, 3 |
| 4.4 `RigProviderAdapter<H>` over an injected backend, base URL validated first, tools declared | 5, 6 |
| 4.5 `rig-core` features `["rustls"]` only, manifest test | 5 |
| 4.6 telemetry slots and attributes | 4, 5 |
| 4.7 tests: `model_profile_catalog.rs`, `model_provider_dispatch.rs` (+ `secret_exclusion.rs`), `rig_provider_fake_endpoint.rs`, gated example walk | 1, 2, 3, 6, 7 |
| 11.1 codes (`model-profile-unknown`, `model-profile-invalid-base-url`, `model-profile-invalid-attribute`, `model-timeout-unset`, `model-credential-missing`, `model-credential-material-unsupported`, `model-router-adapter-version-mismatch`, `model-response-metadata-too-long`) | 1, 2, 3, 4, 6 |
| spec.md 10.1, compat, changelog, matrices, pin table | 8 |

Gaps: none inside the slice. `model-profile-revision-mismatch` is deferred by ruling (refinement 1) and named as unanswered in the compat bullet.

**Placeholder scan:** no "TBD", "TODO", "similar to Task N", or "add validation" text; every code step carries its code. One deliberate "read the tree for the exact method list" instruction remains (Task 6: the `HttpClientExt` methods a counting wrapper delegates), pointing at the file and line rather than leaving the shape open.

**Type consistency:** `AgentModelProfileError::code()` (Task 1) is what Task 6's tests call; `AgentModelError::Refused { code: &'static str, message: String }` (refinement 2) is what Tasks 1, 3, and 6 construct; `AgentGrantedDispatch.tools: Vec<AgentToolDescriptor>` (Task 2) is what Task 3's Model arm copies with `with_tools`; `AgentModelRequest::with_tools(Vec<AgentToolDescriptor>)` (Task 3) is what Tasks 5 and 6 call; `AgentModelResponseMetadata::bounded` (Task 4) is what Task 5's extractors and Task 6 rely on; `RigModelAdapter::with_response_metadata(fn(&M::Response) -> AgentModelResponseMetadata)` (Task 5) is what Task 6's `inner` installs; `World::with_model` (Task 7) is what `run_provider_walk` calls; the four OTel constants (Task 4) are the ones Task 8's matrix row names.
