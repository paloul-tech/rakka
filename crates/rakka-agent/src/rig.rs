//! The Rig-backed model adapter (`rig` feature).
//!
//! Owns the Rig implementation of [`crate::model`]'s adapter trait and the
//! pinned Rig version together with its upgrade review. Rig types never escape
//! this module: they do not appear in the crate's non-`rig` public API, in
//! persisted state, or in A2A metadata, and a raw Rig run is never the durable
//! compatibility format — Rakka persists its own versioned loop representation.
//!
//! Rig memory policies may window, compact, or summarize history, but only
//! behind the Rakka-owned write path, so scoped memory stores and their stable
//! operation identifiers stay authoritative.
//!
//! Specification: sections 10.1 through 10.3. Filled by slice 1.6, which brings
//! the pinned Rig dependency with it. The pin (`rig-core = "=0.37.0"`) and its
//! compatibility review are properties of this feature: a Rig upgrade that
//! changes request, tool-call, message, or serialized run semantics receives an
//! adapter compatibility review, and it never changes the Rakka-owned adapter
//! trait, the core domain types, or the persisted loop representation
//! ([specification 10.2](../../../docs/plans/rakka-agent/spec.md)).
//!
//! # What the adapter maps
//!
//! [`RigModelAdapter`] wraps any Rig [`CompletionModel`] and implements the
//! Rakka-owned [`AgentModelAdapter`]. It turns an [`AgentModelRequest`] into a
//! Rig completion request, calls the provider, and maps the provider's
//! `CompletionResponse` onto a bounded Rakka [`AgentModelTurn`]:
//!
//! - assistant text becomes the turn's text;
//! - a tool call whose name is the adapter's *result tool*
//!   ([`AGENT_RESULT_TOOL_DEFAULT`]) becomes the typed result proposal;
//! - any other tool call becomes a tool-call request — a *request*, never an
//!   authorization: the tool binding and dispatch grant of slice 1.8 decide
//!   whether it may be dispatched
//!   ([specification 11.8](../../../docs/plans/rakka-agent/spec.md)).
//!
//! The result-tool convention is the interim bridge from a provider's
//! function-calling surface to Rakka's typed task result; slice 1.8's tool
//! registry and slice 1.11's context snapshot formalize the surrounding
//! machinery without changing this adapter's contract. The adapter declares the
//! result tool on every completion request it builds — a provider can only call
//! a tool it was offered — while external tools are declared by slice 1.8's
//! registry.
//!
//! A provider client, stream, open request, or credential value is never durable
//! state ([specification 10.1](../../../docs/plans/rakka-agent/spec.md)): the
//! deploying application constructs the concrete `CompletionModel` with its
//! credentials and hands it here, and nothing of it is persisted.

use std::fmt::Display;

use rig_core::completion::{
    AssistantContent, CompletionError, CompletionModel, CompletionRequest, CompletionResponse,
    Message, ToolDefinition, Usage,
};
use rig_core::streaming::StreamingCompletionResponse;
use rig_core::OneOrMany;

use crate::definition::{AgentRevisionNumber, AgentToolId};
use crate::loop_runtime::CURRENT_AGENT_LOOP_ADAPTER_VERSION;
use crate::model::{
    AgentModelAdapter, AgentModelError, AgentModelFuture, AgentModelRequest, AgentModelResult,
    AgentModelRetryPolicy, AgentModelTurn, AgentModelUsage, AgentToolCallId, AgentToolCallRequest,
};
use crate::task::AgentTaskContent;

/// The default name of the tool a model calls to propose its typed task result.
///
/// A provider expresses "I am done, here is the result" as a call to this tool;
/// the adapter maps its arguments onto the run's result proposal rather than
/// treating it as an external effect. A deployment may rename it with
/// [`RigModelAdapter::with_result_tool`].
pub const AGENT_RESULT_TOOL_DEFAULT: &str = "submit_result";

/// Maps a provider or mapping failure onto the model error the adapter returns.
fn provider_error<E: Display>(error: E) -> AgentModelError {
    AgentModelError::Provider {
        message: error.to_string(),
    }
}

/// The Rig-backed implementation of the Rakka-owned [`AgentModelAdapter`].
///
/// It is generic over any Rig [`CompletionModel`], so the deploying application
/// chooses the provider and supplies its credentials; the adapter neither knows
/// nor persists them. The turn it produces is the only durable format for what
/// the model returned — no Rig type crosses this boundary.
#[derive(Debug, Clone)]
pub struct RigModelAdapter<M> {
    model: M,
    adapter_version: AgentRevisionNumber,
    retry_policy: AgentModelRetryPolicy,
    result_tool: String,
}

impl<M> RigModelAdapter<M>
where
    M: CompletionModel,
{
    /// Wraps a Rig completion model with the default adapter version, retry
    /// policy, and result tool.
    #[must_use]
    pub fn new(model: M) -> Self {
        Self {
            model,
            adapter_version: CURRENT_AGENT_LOOP_ADAPTER_VERSION,
            retry_policy: AgentModelRetryPolicy::DEFAULT,
            result_tool: AGENT_RESULT_TOOL_DEFAULT.to_string(),
        }
    }

    /// Stamps a specific adapter version onto the turns this adapter produces.
    #[must_use]
    pub fn with_adapter_version(mut self, adapter_version: AgentRevisionNumber) -> Self {
        self.adapter_version = adapter_version;
        self
    }

    /// Declares the retry policy this adapter's model calls dispatch under,
    /// refusing one the crash-and-timeout rules could not honor
    /// ([specification 11.4](../../../docs/plans/rakka-agent/spec.md)).
    pub fn with_retry_policy(
        mut self,
        retry_policy: AgentModelRetryPolicy,
    ) -> AgentModelResult<Self> {
        retry_policy.validate()?;
        self.retry_policy = retry_policy;
        Ok(self)
    }

    /// Renames the tool a model calls to propose its typed task result.
    #[must_use]
    pub fn with_result_tool(mut self, result_tool: impl Into<String>) -> Self {
        self.result_tool = result_tool.into();
        self
    }

    /// Builds the Rig completion request one model request resolves to.
    ///
    /// The context snapshot is opaque in the interim, so the prompt only names
    /// it; slice 1.11 gives the snapshot content, at which point this assembles a
    /// real prompt from the Rakka-owned write path without changing the adapter
    /// contract.
    ///
    /// The result tool is declared on every request: a provider can only call a
    /// tool it was offered, so without this declaration no real model could ever
    /// express "I am done, here is the result" and no Rig-backed run could
    /// complete. Its argument schema is permissive in the interim — the task's
    /// result schema is judged by the task entity's rules where the proposal is
    /// decided, not trusted to the provider. External tools are declared by the
    /// tool registry of slice 1.8, not here.
    fn build_request(&self, request: &AgentModelRequest) -> CompletionRequest {
        let mut builder = self
            .model
            .completion_request(Message::user(format!("context:{}", request.context)))
            .temperature_opt(
                request
                    .sampling
                    .temperature_milli
                    .map(|milli| f64::from(milli) / 1000.0),
            )
            .max_tokens_opt(request.sampling.max_output_tokens.map(u64::from))
            .tool(ToolDefinition {
                name: self.result_tool.clone(),
                description: "Propose the typed result that completes the task this run serves."
                    .to_string(),
                parameters: serde_json::json!({
                    "type": "object",
                    "additionalProperties": true,
                }),
            });
        // Rig's builder has no first-class nucleus-sampling parameter, so the
        // resolved cutoff rides the provider-parameter escape hatch rather than
        // being silently dropped.
        if let Some(top_p_milli) = request.sampling.top_p_milli {
            builder = builder.additional_params(serde_json::json!({
                "top_p": f64::from(top_p_milli) / 1000.0,
            }));
        }
        builder.build()
    }

    /// Maps a provider response onto a bounded Rakka turn.
    fn turn_from_response<R>(
        &self,
        response: CompletionResponse<R>,
        request: &AgentModelRequest,
    ) -> AgentModelResult<AgentModelTurn> {
        let mut turn =
            AgentModelTurn::new(self.adapter_version).with_usage(model_usage(&response.usage));
        if let Some(profile) = &request.profile {
            turn = turn.with_model_profile(profile.clone());
        }

        let mut text = String::new();
        let mut unmapped_content = false;
        for content in response.choice {
            match content {
                AssistantContent::Text(part) => {
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(&part.text);
                }
                AssistantContent::ToolCall(call) => {
                    if call.function.name == self.result_tool {
                        // Two proposals in one response is an ambiguity the run
                        // must not resolve by whichever the provider ordered
                        // last; the response is refused as a failed effect.
                        if turn.proposal.is_some() {
                            return Err(AgentModelError::Proposal {
                                message: format!(
                                    "the response proposed more than one result via {}",
                                    self.result_tool
                                ),
                            });
                        }
                        let proposal =
                            AgentTaskContent::inline(call.function.arguments).map_err(|error| {
                                AgentModelError::Proposal {
                                    message: error.to_string(),
                                }
                            })?;
                        turn = turn.with_proposal(proposal);
                    } else {
                        let call_id = unique_call_id(
                            call.call_id.clone().unwrap_or_else(|| call.id.clone()),
                            &turn.tool_calls,
                        );
                        let requested = AgentToolCallRequest::new(
                            AgentToolCallId::new(call_id).map_err(provider_error)?,
                            AgentToolId::new(call.function.name).map_err(provider_error)?,
                            call.function.arguments,
                        )?;
                        turn = turn.with_tool_call(requested);
                    }
                }
                // Reasoning and image content have no surface on the M1 durable
                // turn: the turn records text, tool calls, and the proposal, and
                // nothing else. (Telemetry content capture is a separate,
                // default-off concern.)
                _ => unmapped_content = true,
            }
        }
        if !text.is_empty() {
            turn = turn.with_text(text);
        }

        // A response whose only content the durable turn cannot record must not
        // commit as an empty turn: the loop would bill iteration after iteration
        // with nothing durable to show, indistinguishable from a model that
        // chose to do nothing. It is a response the adapter could not map, so it
        // surfaces as a failed effect.
        if unmapped_content
            && turn.text.is_none()
            && turn.proposal.is_none()
            && turn.tool_calls.is_empty()
        {
            return Err(AgentModelError::Provider {
                message: "the provider response carried only content the durable turn does not \
                          record (reasoning or image)"
                    .to_string(),
            });
        }

        turn.validate()?;
        Ok(turn)
    }
}

/// Maps rig's usage onto the durable ledger dimensions without dropping billed
/// tokens.
///
/// Rig's provider conversions disagree on whether cached and reasoning tokens
/// are folded into `input_tokens`/`output_tokens` or reported only in their
/// dedicated fields, but every conversion fills `total_tokens` with the full
/// billed amount — and some providers report *only* a total. The billed input
/// side is therefore recovered as total minus output, which charges cached and
/// reasoning tokens exactly once either way; a usage with no total falls back to
/// summing the dedicated fields.
fn model_usage(usage: &Usage) -> AgentModelUsage {
    let input_tokens = if usage.total_tokens > 0 {
        usage.total_tokens.saturating_sub(usage.output_tokens)
    } else {
        usage
            .input_tokens
            .saturating_add(usage.cached_input_tokens)
            .saturating_add(usage.cache_creation_input_tokens)
            .saturating_add(usage.reasoning_tokens)
    };
    AgentModelUsage {
        input_tokens,
        output_tokens: usage.output_tokens,
        cost_micros: 0,
    }
}

/// Disambiguates a provider call id that collides with one already mapped.
///
/// Some provider conversions reuse the function name as the id of every call
/// (rig's Gemini mapping does), so two parallel calls to one tool would share an
/// id and their results could not be told apart. The loop matches a result to
/// its request by this id, so it must be unique within the turn.
fn unique_call_id(candidate: String, taken: &[AgentToolCallRequest]) -> String {
    let is_taken = |id: &str, taken: &[AgentToolCallRequest]| {
        taken.iter().any(|call| call.call_id.as_str() == id)
    };
    if !is_taken(&candidate, taken) {
        return candidate;
    }
    let mut suffix = 2_usize;
    loop {
        let disambiguated = format!("{candidate}-{suffix}");
        if !is_taken(&disambiguated, taken) {
            return disambiguated;
        }
        suffix += 1;
    }
}

impl<M> AgentModelAdapter for RigModelAdapter<M>
where
    M: CompletionModel,
{
    fn adapter_version(&self) -> AgentRevisionNumber {
        self.adapter_version
    }

    fn retry_policy(&self) -> AgentModelRetryPolicy {
        self.retry_policy
    }

    fn call<'a>(&'a self, request: &'a AgentModelRequest) -> AgentModelFuture<'a> {
        Box::pin(async move {
            let rig_request = self.build_request(request);
            let response = self
                .model
                .completion(rig_request)
                .await
                .map_err(provider_error)?;
            self.turn_from_response(response, request)
        })
    }
}

/// A deterministic Rig [`CompletionModel`] that returns scripted responses.
///
/// It is the fake provider the Rig adapter composes with in tests
/// ([specification 10.4](../../../docs/plans/rakka-agent/spec.md)): no network,
/// no credentials, no provider account — just a canned `CompletionResponse` the
/// [`RigModelAdapter`] maps exactly as it maps a real provider's, so a Rig-backed
/// run travels the same durable effect path a deterministic one does. It is part
/// of the `rig` feature's public surface, never the non-`rig` API.
#[derive(Debug, Clone, Default)]
pub struct ScriptedCompletionModel {
    choice: Vec<AssistantContent>,
    usage: Usage,
}

impl ScriptedCompletionModel {
    /// An empty script. A completion with nothing scripted returns one line of
    /// placeholder text, so the adapter still produces a bounded turn.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Scripts a line of assistant text.
    #[must_use]
    pub fn returning_text(mut self, text: impl Into<String>) -> Self {
        self.choice.push(AssistantContent::text(text));
        self
    }

    /// Scripts a call to the *default* result tool ([`AGENT_RESULT_TOOL_DEFAULT`])
    /// carrying the proposed task result, which the [`RigModelAdapter`] maps onto
    /// a typed result proposal.
    ///
    /// The adapter matches proposals by its configured result-tool name, so a
    /// script for an adapter renamed with [`RigModelAdapter::with_result_tool`]
    /// must use [`Self::returning_result_as`] with the same name — a call under
    /// the default name would map to an ordinary tool-call request instead.
    #[must_use]
    pub fn returning_result(self, content: serde_json::Value) -> Self {
        self.returning_result_as(AGENT_RESULT_TOOL_DEFAULT, content)
    }

    /// Scripts a call to `result_tool` carrying the proposed task result.
    ///
    /// This is the [`Self::returning_result`] for an adapter whose result tool
    /// was renamed with [`RigModelAdapter::with_result_tool`]: the scripted name
    /// must match the adapter's, or the call maps to a tool-call request rather
    /// than a proposal.
    #[must_use]
    pub fn returning_result_as(
        mut self,
        result_tool: impl Into<String>,
        content: serde_json::Value,
    ) -> Self {
        self.choice.push(AssistantContent::tool_call(
            "submit-result-call",
            result_tool.into(),
            content,
        ));
        self
    }

    /// Scripts a tool call the model requests.
    #[must_use]
    pub fn requesting_tool(
        mut self,
        call_id: impl Into<String>,
        tool: impl Into<String>,
        arguments: serde_json::Value,
    ) -> Self {
        self.choice
            .push(AssistantContent::tool_call(call_id, tool, arguments));
        self
    }

    /// Reports the token usage the scripted completion bills.
    #[must_use]
    pub fn with_usage(mut self, input_tokens: u64, output_tokens: u64) -> Self {
        self.usage = Usage {
            input_tokens,
            output_tokens,
            total_tokens: input_tokens.saturating_add(output_tokens),
            cached_input_tokens: 0,
            cache_creation_input_tokens: 0,
            reasoning_tokens: 0,
        };
        self
    }

    fn response_choice(&self) -> OneOrMany<AssistantContent> {
        OneOrMany::many(self.choice.clone())
            .unwrap_or_else(|_| OneOrMany::one(AssistantContent::text("(no scripted response)")))
    }
}

impl CompletionModel for ScriptedCompletionModel {
    type Response = ();
    // The scripted model does not stream, and rig implements the streaming
    // bounds (`GetTokenUsage` included) for the unit type.
    type StreamingResponse = ();
    type Client = ();

    fn make(_client: &Self::Client, _model: impl Into<String>) -> Self {
        Self::new()
    }

    async fn completion(
        &self,
        _request: CompletionRequest,
    ) -> Result<CompletionResponse<()>, CompletionError> {
        Ok(CompletionResponse {
            choice: self.response_choice(),
            usage: self.usage,
            raw_response: (),
            message_id: None,
        })
    }

    async fn stream(
        &self,
        _request: CompletionRequest,
    ) -> Result<StreamingCompletionResponse<Self::StreamingResponse>, CompletionError> {
        Err(CompletionError::ProviderError(
            "the scripted completion model does not stream".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::AgentModelProfileId;
    use crate::identity::{AgentId, AgentRunId, TenantId};
    use crate::memory::AgentContextSnapshotRef;

    fn request() -> AgentModelRequest {
        let scope = crate::identity::AgentRunScope::new(
            TenantId::new("acme"),
            AgentId::new("support").expect("the agent id is valid"),
            AgentRunId::new("t-gen-1").expect("the run id is valid"),
        )
        .expect("the scope is valid");
        AgentModelRequest::new(
            AgentContextSnapshotRef::for_turn(&scope, 1).expect("the reference derives"),
            1,
        )
    }

    #[tokio::test]
    async fn a_result_tool_call_becomes_a_typed_proposal() {
        let adapter = RigModelAdapter::new(
            ScriptedCompletionModel::new()
                .returning_text("done")
                .returning_result(serde_json::json!({ "answer": "resolved" }))
                .with_usage(10, 5),
        );
        let turn = adapter.call(&request()).await.expect("the turn maps");

        assert_eq!(turn.text.as_deref(), Some("done"));
        assert_eq!(
            turn.proposal.expect("a proposal").inline_value(),
            Some(&serde_json::json!({ "answer": "resolved" }))
        );
        assert!(
            turn.tool_calls.is_empty(),
            "the result tool is not a tool call"
        );
        assert_eq!(turn.usage.total_tokens(), 15);
    }

    #[tokio::test]
    async fn a_non_result_tool_call_becomes_a_tool_request() {
        let adapter = RigModelAdapter::new(ScriptedCompletionModel::new().requesting_tool(
            "call-1",
            "knowledge-base",
            serde_json::json!({ "query": "ticket" }),
        ));
        let turn = adapter.call(&request()).await.expect("the turn maps");

        assert_eq!(turn.tool_calls.len(), 1);
        assert!(turn.proposal.is_none());
    }

    #[tokio::test]
    async fn the_profile_the_request_selected_is_stamped_on_the_turn() {
        let adapter = RigModelAdapter::new(ScriptedCompletionModel::new().returning_text("hi"));
        let request = request().with_profile(
            AgentModelProfileId::new("gpt-provider").expect("the profile id is valid"),
        );
        let turn = adapter.call(&request).await.expect("the turn maps");
        assert_eq!(
            turn.model_profile.expect("a profile").as_str(),
            "gpt-provider"
        );
    }

    #[test]
    fn the_result_tool_and_sampling_reach_the_completion_request() {
        use crate::definition::AgentSamplingSettings;

        let adapter = RigModelAdapter::new(ScriptedCompletionModel::new());
        let request = request().with_sampling(AgentSamplingSettings {
            temperature_milli: Some(200),
            top_p_milli: Some(900),
            max_output_tokens: Some(64),
        });
        let built = adapter.build_request(&request);

        // The provider is offered the result tool; without the declaration no
        // real model could ever call it, and no run could complete.
        assert!(
            built
                .tools
                .iter()
                .any(|tool| tool.name == AGENT_RESULT_TOOL_DEFAULT),
            "the result tool is declared on the request"
        );
        assert_eq!(built.temperature, Some(0.2));
        assert_eq!(built.max_tokens, Some(64));
        // Nucleus sampling rides the provider-parameter escape hatch.
        let params = built.additional_params.expect("top_p is forwarded");
        assert_eq!(params.get("top_p"), Some(&serde_json::json!(0.9)));
    }

    #[tokio::test]
    async fn a_renamed_result_tool_composes_with_the_scripted_provider() {
        let adapter = RigModelAdapter::new(
            ScriptedCompletionModel::new()
                .returning_result_as("finish", serde_json::json!({ "answer": "resolved" })),
        )
        .with_result_tool("finish");
        let turn = adapter.call(&request()).await.expect("the turn maps");

        assert!(turn.proposal.is_some(), "the renamed result tool proposes");
        assert!(turn.tool_calls.is_empty());
    }

    #[tokio::test]
    async fn a_response_with_two_result_proposals_is_refused_as_ambiguous() {
        let adapter = RigModelAdapter::new(
            ScriptedCompletionModel::new()
                .returning_result(serde_json::json!({ "answer": "draft" }))
                .returning_result(serde_json::json!({ "answer": "final" })),
        );
        let error = adapter
            .call(&request())
            .await
            .expect_err("two proposals are ambiguous");
        assert_eq!(error.code(), "model-proposal-not-bounded");
    }

    #[tokio::test]
    async fn text_blocks_are_separated_rather_than_glued_together() {
        let adapter = RigModelAdapter::new(
            ScriptedCompletionModel::new()
                .returning_text("Checking the KB.")
                .returning_text("Done."),
        );
        let turn = adapter.call(&request()).await.expect("the turn maps");
        assert_eq!(turn.text.as_deref(), Some("Checking the KB.\nDone."));
    }

    #[tokio::test]
    async fn colliding_provider_call_ids_are_disambiguated() {
        // Rig's Gemini conversion reuses the function name as every call's id,
        // so two parallel calls to one tool arrive with the same id.
        let adapter = RigModelAdapter::new(
            ScriptedCompletionModel::new()
                .requesting_tool("search", "search", serde_json::json!({ "query": "a" }))
                .requesting_tool("search", "search", serde_json::json!({ "query": "b" })),
        );
        let turn = adapter.call(&request()).await.expect("the turn maps");

        assert_eq!(turn.tool_calls.len(), 2);
        assert_eq!(turn.tool_calls[0].call_id.as_str(), "search");
        assert_eq!(turn.tool_calls[1].call_id.as_str(), "search-2");
        assert_eq!(
            turn.tool_calls[1].arguments,
            serde_json::json!({ "query": "b" }),
            "each result stays correlated to the call that asked for it"
        );
    }

    #[tokio::test]
    async fn a_reasoning_only_response_fails_the_mapping_instead_of_committing_nothing() {
        let mut provider = ScriptedCompletionModel::new();
        provider
            .choice
            .push(AssistantContent::reasoning("thinking..."));
        let adapter = RigModelAdapter::new(provider);
        let error = adapter
            .call(&request())
            .await
            .expect_err("a turn with nothing durable is refused, not billed");
        assert_eq!(error.code(), "model-provider-failed");
    }

    #[test]
    fn billed_tokens_are_recovered_from_the_provider_total() {
        // Anthropic-style: cached and cache-creation input excluded from
        // input_tokens but included in the total.
        let anthropic = Usage {
            input_tokens: 100,
            output_tokens: 20,
            total_tokens: 5220,
            cached_input_tokens: 5000,
            cache_creation_input_tokens: 100,
            reasoning_tokens: 0,
        };
        assert_eq!(model_usage(&anthropic).total_tokens(), 5220);

        // OpenAI-style: cached input already folded into input_tokens, so the
        // total-based recovery must not double-count it.
        let openai = Usage {
            input_tokens: 1000,
            output_tokens: 50,
            total_tokens: 1050,
            cached_input_tokens: 400,
            cache_creation_input_tokens: 0,
            reasoning_tokens: 30,
        };
        assert_eq!(model_usage(&openai).total_tokens(), 1050);

        // A provider that reports only a total still bills it.
        let total_only = Usage {
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 5000,
            cached_input_tokens: 0,
            cache_creation_input_tokens: 0,
            reasoning_tokens: 0,
        };
        assert_eq!(model_usage(&total_only).total_tokens(), 5000);
    }

    #[test]
    fn an_invalid_retry_policy_is_refused_at_adapter_construction() {
        use crate::definition::AgentEffectSafetyClass;

        let error = RigModelAdapter::new(ScriptedCompletionModel::new())
            .with_retry_policy(crate::model::AgentModelRetryPolicy {
                safety_class: AgentEffectSafetyClass::NonIdempotent,
                max_attempts: 3,
            })
            .expect_err("an unenforceable policy is refused where it is declared");
        assert_eq!(error.code(), "model-retry-policy-invalid");
    }
}
