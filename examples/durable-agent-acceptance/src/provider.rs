//! The gated live-provider walk: the same world, task, tool, and envelope as
//! the acceptance walk, driven through the real Rig provider adapter over an
//! env-backed credential resolver that lives here, in the example, and never
//! in a crate.
//!
//! Nothing here runs unless the environment arms it. With `RAKKA_MODEL_PROFILE`
//! unset, [`run_provider_walk`] answers the variable that is missing and the
//! walk is skipped; the integration test reports the skip and passes, and the
//! binary exits 2.
//!
//! What the walk asserts is structural, not a transcript: the 18-line
//! acceptance transcript is scripted around content sentinels and a
//! deterministic two-turn flow, and a live model cannot reproduce it. The run
//! reaches a terminal status, the provider reports the model that answered,
//! the recording tool executor's invocations are reported, and no byte of the
//! resolved provider key reaches a durable record or a line of output.

use std::collections::BTreeMap;
use std::sync::Arc;

use rakka_a2a::agents::A2AAgentTarget;
use rakka_agent::rig::{ReqwestClient, RigProviderAdapter};
use rakka_agent::{
    AgentCredentialBindingRef, AgentDispatchError, AgentDispatchFuture,
    AgentEffectCredentialResolver, AgentModelCapabilities, AgentModelProfile, AgentModelProfileId,
    AgentModelProviderKind, AgentRevisionNumber, AgentRunEffect, AgentRunScope,
    AgentSamplingSettings, StaticAgentModelProfileCatalog,
};
use rakka_agent_workflow::AgentEphemeralCredential;

use crate::wiring::World;

/// The environment variable naming the profile id, and the gate on the whole
/// walk: with it unset, nothing here runs.
pub const PROVIDER_PROFILE_VAR: &str = "RAKKA_MODEL_PROFILE";
/// The environment variable naming the provider kind.
pub const PROVIDER_KIND_VAR: &str = "RAKKA_MODEL_PROVIDER";
/// The environment variable naming the provider's model.
pub const PROVIDER_MODEL_VAR: &str = "RAKKA_MODEL_NAME";
/// The environment variable overriding the provider's endpoint.
pub const PROVIDER_BASE_URL_VAR: &str = "RAKKA_MODEL_BASE_URL";
/// The environment variable holding the provider key.
///
/// Read only inside [`EnvCredentialResolver::resolve`], which hands the value
/// straight to the ephemeral credential the dispatcher drops with the attempt,
/// and by the walk's own exclusion sweep, which compares and discards it.
pub const PROVIDER_KEY_VAR: &str = "RAKKA_MODEL_API_KEY";

/// The logical binding the profile names when the walk has a key.
const BINDING: &str = "live-provider-key";

/// The per-attempt bound the walk gives model effects.
///
/// A credential-bearing model call with no `timeout_ms` is refused
/// `model-timeout-unset` at the authority: the resolver's only deadline input
/// is the effect's own bound, so an unbounded attempt could hold a live secret
/// open indefinitely.
const MODEL_TIMEOUT_MS: u64 = 60_000;

/// Resolves one logical binding from one environment variable, read at resolve
/// time.
///
/// The value lives only in the [`AgentEphemeralCredential`] the dispatcher
/// drops with the attempt: this struct holds the variable's *name*, never its
/// value, so no field of it, and no `Debug` of it, can carry a secret.
#[derive(Debug)]
pub struct EnvCredentialResolver {
    var: String,
    kind: AgentModelProviderKind,
}

impl EnvCredentialResolver {
    /// A resolver that reads `var` for the given provider kind.
    #[must_use]
    pub fn new(var: impl Into<String>, kind: AgentModelProviderKind) -> Self {
        Self {
            var: var.into(),
            kind,
        }
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
            // The error text a failing resolution returns becomes durable
            // state, so it names the variable and never what it held.
            let value = std::env::var(&self.var).map_err(|_error| {
                AgentDispatchError::collaborator(
                    "credential-env-unset",
                    format!("{} is not set", self.var),
                )
            })?;
            Ok(match self.kind {
                AgentModelProviderKind::Anthropic => {
                    AgentEphemeralCredential::api_key("x-api-key", value)
                }
                _ => AgentEphemeralCredential::bearer_token(value),
            })
        })
    }
}

/// What the gated walk reports.
#[derive(Debug)]
pub struct ProviderWalkReport {
    /// One line per fact, `ok  …` when it held.
    pub lines: Vec<String>,
    /// The provider kind the walk drove.
    ///
    /// Reported because it decides what a caller may hold the walk to: rig
    /// 0.37 carries a provider's own response metadata only where the adapter
    /// knows its raw response type, so [`Self::response_model`] is filled for
    /// [`AgentModelProviderKind::Anthropic`],
    /// [`AgentModelProviderKind::OpenAiCompletions`] and
    /// [`AgentModelProviderKind::Custom`] (the Chat Completions shape), and is
    /// absent for every other kind however well the call went.
    pub provider: AgentModelProviderKind,
    /// The response model the provider reported on the first recorded turn.
    pub response_model: Option<String>,
    /// How many times the recording tool executor was invoked.
    pub tool_invocations: usize,
}

/// Whether rig 0.37 carries this provider's own response metadata back.
///
/// The adapter reads a response model and finish reason only where it knows
/// the provider's raw response type; every other route keeps both inside
/// rig's generic `CompletionResponse<T>`, with no extractor installed.
#[must_use]
pub const fn provider_reports_its_model(provider: &AgentModelProviderKind) -> bool {
    matches!(
        provider,
        AgentModelProviderKind::Anthropic
            | AgentModelProviderKind::OpenAiCompletions
            | AgentModelProviderKind::Custom(_)
    )
}

/// The provider kind one `RAKKA_MODEL_PROVIDER` label names.
fn provider_kind(name: &str) -> Result<AgentModelProviderKind, String> {
    Ok(match name {
        "anthropic" => AgentModelProviderKind::Anthropic,
        "openai-completions" => AgentModelProviderKind::OpenAiCompletions,
        "openai-responses" => AgentModelProviderKind::OpenAiResponses,
        "openrouter" => AgentModelProviderKind::OpenRouter,
        "gemini" => AgentModelProviderKind::Gemini,
        "ollama" => AgentModelProviderKind::Ollama,
        "custom" => AgentModelProviderKind::Custom("custom".to_string()),
        other => {
            return Err(format!(
                "{PROVIDER_KIND_VAR}={other} is not a known provider kind"
            ))
        }
    })
}

/// The profile the environment describes.
///
/// # Errors
///
/// The gate variable that is unset, or a profile the record's own bounds
/// refuse.
pub fn provider_profile_from_env() -> Result<AgentModelProfile, String> {
    let profile_id = std::env::var(PROVIDER_PROFILE_VAR).map_err(|_error| {
        format!("{PROVIDER_PROFILE_VAR} is unset; the live provider walk is skipped")
    })?;
    let provider = provider_kind(
        &std::env::var(PROVIDER_KIND_VAR)
            .map_err(|_error| format!("{PROVIDER_KIND_VAR} is unset"))?,
    )?;
    let model = std::env::var(PROVIDER_MODEL_VAR)
        .map_err(|_error| format!("{PROVIDER_MODEL_VAR} is unset"))?;
    // Ollama serves unauthenticated by default; every other kind needs the
    // key, and a key set beside Ollama is honored rather than ignored.
    let keyed = !matches!(provider, AgentModelProviderKind::Ollama)
        || std::env::var(PROVIDER_KEY_VAR).is_ok_and(|key| !key.is_empty());
    let binding = keyed
        .then(|| AgentCredentialBindingRef::new(BINDING))
        .transpose()
        .map_err(|error| format!("the binding reference is invalid: {error}"))?;
    let profile = AgentModelProfile {
        profile_id: AgentModelProfileId::new(&profile_id)
            .map_err(|error| format!("{PROVIDER_PROFILE_VAR}: {error}"))?,
        revision: AgentRevisionNumber::INITIAL,
        provider,
        model,
        base_url: std::env::var(PROVIDER_BASE_URL_VAR).ok(),
        credential_binding: binding,
        default_sampling: AgentSamplingSettings::default(),
        capabilities: AgentModelCapabilities { tool_calls: true },
        attributes: BTreeMap::new(),
    };
    profile
        .validate()
        .map_err(|error| format!("the profile from the environment is invalid: {error}"))?;
    Ok(profile)
}

/// Runs the world's run through the live provider and reports the facts.
///
/// # Errors
///
/// The gate variable that is unset — which is the skip, not a failure — an
/// invalid profile, a profile the adapter refuses, a run that does not
/// terminate, or a key that reached a durable record.
pub async fn run_provider_walk() -> Result<ProviderWalkReport, String> {
    let profile = provider_profile_from_env()?;
    let adapter = RigProviderAdapter::new(profile.clone(), ReqwestClient::new())
        .map_err(|error| format!("the provider adapter refused the profile: {error}"))?;
    let catalog = StaticAgentModelProfileCatalog::new()
        .with_profile(profile.clone())
        .map_err(|error| format!("the catalog refused the profile: {error}"))?;
    let credentials: Option<Arc<dyn AgentEffectCredentialResolver>> =
        profile.credential_binding.as_ref().map(|_binding| {
            Arc::new(EnvCredentialResolver::new(
                PROVIDER_KEY_VAR,
                profile.provider.clone(),
            )) as Arc<dyn AgentEffectCredentialResolver>
        });
    let world = World::with_model(
        Arc::new(adapter),
        Some(Arc::new(catalog)),
        credentials,
        Some(MODEL_TIMEOUT_MS),
        A2AAgentTarget::new(crate::flow::agent_id(), crate::flow::task_definition()),
    );
    let report = crate::flow::drive_one_run_with_profile(&world, &profile).await;
    world.system.shutdown();
    report
}
