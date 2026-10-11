//! The Rig provider adapter against in-process fakes of the two provider
//! APIs it reads provenance from, over an injected HTTP backend. No network,
//! no key: the credential is an ephemeral sentinel that must appear on the
//! wire the fake sees and nowhere else.
#![cfg(feature = "rig")]

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{RawQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Json, Router};
use bytes::Bytes;
use serde_json::{json, Value};

use rakka_agent::rig::RigProviderAdapter;
use rakka_agent::{
    AgentContextSnapshotId, AgentContextSnapshotRef, AgentModelAdapter, AgentModelCapabilities,
    AgentModelProfile, AgentModelProfileId, AgentModelProviderKind, AgentModelRequest,
    AgentRevisionNumber, AgentSamplingSettings, AgentSchemaId, AgentSchemaRef, AgentToolDescriptor,
    AgentToolId, AgentToolKind,
};
use rakka_agent_workflow::AgentEphemeralCredential;
use rig_core::http_client::{
    self, HttpClientExt, LazyBody, MultipartForm, ReqwestClient, StreamingResponse,
};
use rig_core::wasm_compat::WasmCompatSend;

/// The marker a request's context reference carries when the fake completions
/// endpoint should answer with a tool call rather than text.
///
/// The adapter's prompt is derived from the context reference, so this is the
/// only part of the request a test controls that reaches the wire — which also
/// makes the assertion below proof that the prompt itself travels.
const ASK_FOR_A_TOOL_CALL: &str = "call-a-tool";

#[derive(Clone, Default)]
struct Seen {
    bodies: Arc<Mutex<Vec<Value>>>,
    headers: Arc<Mutex<Vec<HeaderMap>>>,
    /// The raw query string of each request, recorded by the routes whose
    /// providers carry part of the call in the URL rather than the body.
    queries: Arc<Mutex<Vec<String>>>,
    hits: Arc<AtomicUsize>,
}

/// The longest tool name Anthropic accepts: `^[a-zA-Z0-9_-]{1,128}$` (Claude
/// API, "Define tools").
const ANTHROPIC_TOOL_NAME_MAX_BYTES: usize = 128;

/// The longest tool name OpenAI accepts: `[a-zA-Z0-9_-]`, at most 64
/// (`openai/openai-openapi`, `FunctionObject.name`).
const OPENAI_TOOL_NAME_MAX_BYTES: usize = 64;

/// The tool names a request body declares, in either wire shape: Anthropic's
/// `tools[].name` or Chat Completions' `tools[].function.name`.
fn declared_tool_names(body: &Value) -> Vec<String> {
    body["tools"]
        .as_array()
        .map(|tools| {
            tools
                .iter()
                .filter_map(|tool| {
                    tool["name"]
                        .as_str()
                        .or_else(|| tool["function"]["name"].as_str())
                })
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// The first declared tool name outside a provider's rule.
///
/// Both real providers refuse the **whole request** with a `400` over one
/// such name, so both fakes do too. A fake that took any name could never
/// show the adapter declaring one a provider refuses, which is how an MCP
/// tool id (`mcp.<server>.<tool>`) failed every first turn unseen.
fn name_outside_the_rule(body: &Value, max_bytes: usize) -> Option<String> {
    declared_tool_names(body).into_iter().find(|name| {
        name.is_empty()
            || name.len() > max_bytes
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    })
}

/// The name a fake model calls when asked for a tool: the first declared
/// tool other than the result tool, exactly as it was declared — a real
/// model can only call a name it was offered.
fn offered_tool(body: &Value) -> String {
    declared_tool_names(body)
        .into_iter()
        .find(|name| name != "submit_result")
        .unwrap_or_default()
}

async fn anthropic_messages(
    State(seen): State<Seen>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    seen.hits.fetch_add(1, Ordering::SeqCst);
    seen.headers.lock().expect("not poisoned").push(headers);
    let refused = name_outside_the_rule(&body, ANTHROPIC_TOOL_NAME_MAX_BYTES);
    let wants_tool = body["messages"].to_string().contains(ASK_FOR_A_TOOL_CALL);
    let called = offered_tool(&body);
    seen.bodies.lock().expect("not poisoned").push(body);
    if let Some(name) = refused {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "type": "error",
                "error": {
                    "type": "invalid_request_error",
                    "message": format!("tools: {name:?} should match pattern '^[a-zA-Z0-9_-]{{1,128}}$'"),
                }
            })),
        );
    }
    let content = if wants_tool {
        json!([{ "type": "tool_use", "id": "toolu_1", "name": called, "input": { "q": "refunds" } }])
    } else {
        json!([{ "type": "text", "text": "hello from the fake" }])
    };
    (
        StatusCode::OK,
        Json(json!({
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "claude-sonnet-5-20260901",
            "content": content,
            "stop_reason": if wants_tool { "tool_use" } else { "end_turn" },
            "stop_sequence": null,
            "usage": { "input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": 3, "cache_creation_input_tokens": 0 }
        })),
    )
}

async fn openai_chat_completions(
    State(seen): State<Seen>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    seen.hits.fetch_add(1, Ordering::SeqCst);
    seen.headers.lock().expect("not poisoned").push(headers);
    let refused = name_outside_the_rule(&body, OPENAI_TOOL_NAME_MAX_BYTES);
    let wants_tool = body["messages"].to_string().contains(ASK_FOR_A_TOOL_CALL);
    let called = offered_tool(&body);
    seen.bodies.lock().expect("not poisoned").push(body);
    if let Some(name) = refused {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": {
                    "message": format!("Invalid 'tools.function.name': string {name:?} does not match pattern. Expected a string that matches the pattern '^[a-zA-Z0-9_-]+$'."),
                    "type": "invalid_request_error",
                    "param": "tools.function.name",
                    "code": "invalid_value",
                }
            })),
        );
    }
    let message = if wants_tool {
        json!({ "role": "assistant", "content": null, "tool_calls": [{ "id": "call_1", "type": "function", "function": { "name": called, "arguments": "{\"q\":\"refunds\"}" } }] })
    } else {
        json!({ "role": "assistant", "content": "hello from the fake" })
    };
    (
        StatusCode::OK,
        Json(json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "created": 1,
            "model": "gpt-fake-1",
            "choices": [{ "index": 0, "message": message, "finish_reason": if wants_tool { "tool_calls" } else { "stop" } }],
            "usage": { "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
                       "prompt_tokens_details": { "cached_tokens": 3 }, "completion_tokens_details": { "reasoning_tokens": 2 } }
        })),
    )
}

/// Azure OpenAI's chat-completions route: the Chat Completions request and
/// response shape (`rig-core-0.37.0/src/providers/azure.rs:688`), under Azure's
/// own deployment path (`:251-262`), recording the query so a test can see the
/// `api-version` the profile attribute chose.
async fn azure_chat_completions(
    State(seen): State<Seen>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Json<Value>,
) -> (StatusCode, Json<Value>) {
    seen.queries
        .lock()
        .expect("not poisoned")
        .push(query.unwrap_or_default());
    openai_chat_completions(State(seen), headers, body).await
}

/// The sentinel a refusing provider echoes back inside its 400 body, standing
/// for the request content a real provider's 4xx quotes at you.
const LEAKED_PROMPT_SENTINEL: &str = "leaked-prompt-sentinel";

/// A provider that rejects the call and quotes the request back in the body —
/// exactly what an OpenAI 400 does with `invalid_request_error`.
async fn refusing_chat_completions(State(seen): State<Seen>) -> (StatusCode, Json<Value>) {
    seen.hits.fetch_add(1, Ordering::SeqCst);
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "error": {
                "type": "invalid_request_error",
                "message": format!("your request was rejected: {LEAKED_PROMPT_SENTINEL}"),
            }
        })),
    )
}

async fn serve(seen: Seen) -> SocketAddr {
    let app = Router::new()
        .route("/v1/messages", post(anthropic_messages))
        .route("/v1/chat/completions", post(openai_chat_completions))
        .route(
            "/openai/deployments/{deployment}/chat/completions",
            post(azure_chat_completions),
        )
        .route(
            "/refuse/v1/chat/completions",
            post(refusing_chat_completions),
        )
        .with_state(seen);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("binds");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await.expect("serves") });
    addr
}

fn profile(
    kind: AgentModelProviderKind,
    model: &str,
    base_url: Option<String>,
) -> AgentModelProfile {
    profile_with_attributes(kind, model, base_url, BTreeMap::new())
}

fn profile_with_attributes(
    kind: AgentModelProviderKind,
    model: &str,
    base_url: Option<String>,
    attributes: BTreeMap<String, String>,
) -> AgentModelProfile {
    AgentModelProfile {
        profile_id: AgentModelProfileId::new("fake").expect("id"),
        revision: AgentRevisionNumber::INITIAL,
        provider: kind,
        model: model.to_string(),
        base_url,
        credential_binding: None,
        default_sampling: AgentSamplingSettings::default(),
        capabilities: AgentModelCapabilities { tool_calls: true },
        attributes,
    }
}

fn request(marker: &str) -> AgentModelRequest {
    request_offering(marker, "search_kb", AgentToolKind::Function)
}

/// A request offering one model-visible tool, `tool`, beside the result tool.
fn request_offering(marker: &str, tool: &str, kind: AgentToolKind) -> AgentModelRequest {
    let descriptor = AgentToolDescriptor::new(
        AgentToolId::new(tool).expect("tool id"),
        kind,
        "Searches the knowledge base.",
        AgentSchemaRef::new(
            AgentSchemaId::new("kb-input").expect("schema"),
            AgentRevisionNumber::INITIAL,
        ),
        AgentSchemaRef::new(
            AgentSchemaId::new("kb-output").expect("schema"),
            AgentRevisionNumber::INITIAL,
        ),
    )
    .expect("descriptor");
    AgentModelRequest::new(
        AgentContextSnapshotRef::new(
            AgentContextSnapshotId::new(marker).expect("ref"),
            AgentRevisionNumber::INITIAL,
        ),
        1,
    )
    // Anthropic's Messages API requires `max_tokens`, and rig refuses the
    // request rather than inventing one for a model it does not recognize
    // (`rig-core-0.37.0/src/providers/anthropic/completion.rs:1419-1428`).
    .with_sampling(AgentSamplingSettings {
        temperature_milli: None,
        top_p_milli: None,
        max_output_tokens: Some(256),
    })
    .with_tools(vec![descriptor])
}

#[tokio::test]
async fn the_anthropic_round_trip_carries_the_key_the_tools_and_the_provenance() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let adapter = RigProviderAdapter::new(
        profile(
            AgentModelProviderKind::Anthropic,
            "claude-sonnet-5",
            Some(format!("http://{addr}")),
        ),
        ReqwestClient::new(),
    )
    .expect("valid");
    let credential = AgentEphemeralCredential::api_key("x-api-key", "sk-ant-fake-sentinel");

    let turn = adapter
        .call_with(&request("say-hello"), Some(&credential))
        .await
        .expect("answers");
    assert_eq!(turn.text.as_deref(), Some("hello from the fake"));
    assert_eq!(
        turn.response_model.as_deref(),
        Some("claude-sonnet-5-20260901")
    );
    assert_eq!(turn.finish_reason.as_deref(), Some("end_turn"));
    assert_eq!(
        turn.usage.input_tokens, 13,
        "cached tokens are billed as input, as before"
    );
    assert_eq!(turn.usage.cached_input_tokens, Some(3));

    let headers = seen.headers.lock().expect("not poisoned");
    assert_eq!(
        headers[0].get("x-api-key").and_then(|v| v.to_str().ok()),
        Some("sk-ant-fake-sentinel")
    );
    let body = &seen.bodies.lock().expect("not poisoned")[0];
    let tools: Vec<&str> = body["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    assert!(
        tools.contains(&"search_kb") && tools.contains(&"submit_result"),
        "{tools:?}"
    );
    assert_eq!(body["model"], "claude-sonnet-5");
}

#[tokio::test]
async fn the_openai_completions_round_trip_carries_the_bearer_and_maps_a_tool_call() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let adapter = RigProviderAdapter::new(
        profile(
            AgentModelProviderKind::OpenAiCompletions,
            "gpt-fake-1",
            Some(format!("http://{addr}/v1")),
        ),
        ReqwestClient::new(),
    )
    .expect("valid");
    let credential = AgentEphemeralCredential::bearer_token("sk-openai-fake-sentinel");

    let turn = adapter
        .call_with(&request(ASK_FOR_A_TOOL_CALL), Some(&credential))
        .await
        .expect("answers");
    assert_eq!(turn.tool_calls.len(), 1);
    assert_eq!(turn.tool_calls[0].tool.as_str(), "search_kb");
    assert_eq!(turn.tool_calls[0].arguments["q"], "refunds");
    assert_eq!(turn.response_model.as_deref(), Some("gpt-fake-1"));
    assert_eq!(turn.finish_reason.as_deref(), Some("tool_calls"));
    assert_eq!(turn.usage.cached_input_tokens, Some(3));
    // Rig's Chat Completions conversion hard-codes `reasoning_tokens: 0` and
    // its `Usage` has no `completion_tokens_details` field at all
    // (`rig-core-0.37.0/src/providers/openai/completion/mod.rs:970-984`,
    // `:1077-1082`), so a provider that reports reasoning tokens on that
    // endpoint cannot reach the durable ledger through this adapter.
    assert_eq!(turn.usage.reasoning_tokens, None);

    // The fake answers a tool call on the marker alone, so the mapping above
    // proves nothing about the declaration. This does: the wire body carries
    // both tools, under the Chat Completions `{type, function: {name}}` shape
    // (`rig-core-0.37.0/src/providers/openai/completion/mod.rs:390-402`).
    let body = &seen.bodies.lock().expect("not poisoned")[0];
    let tools: Vec<&str> = body["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .filter_map(|tool| tool["function"]["name"].as_str())
        .collect();
    assert!(
        tools.contains(&"search_kb") && tools.contains(&"submit_result"),
        "{tools:?}"
    );

    let headers = seen.headers.lock().expect("not poisoned");
    assert_eq!(
        headers[0]
            .get("authorization")
            .and_then(|v| v.to_str().ok()),
        Some("Bearer sk-openai-fake-sentinel")
    );
}

/// Issue #85 ask 16. An MCP tool id (`mcp.<server>.<tool>`) is outside both
/// providers' name rules, and each refuses the whole request over it, so
/// before the fix any run offering an MCP tool failed its first turn. The
/// adapter declares it under its provider name, and reads the call the
/// provider makes under that name back to the id, on both wire shapes.
#[tokio::test]
async fn an_mcp_tool_is_declared_under_a_name_the_provider_accepts_and_read_back_to_its_id() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let routes = [
        (
            AgentModelProviderKind::Anthropic,
            "claude-sonnet-5",
            format!("http://{addr}"),
            AgentEphemeralCredential::api_key("x-api-key", "sk-ant-fake-sentinel"),
        ),
        (
            AgentModelProviderKind::OpenAiCompletions,
            "gpt-fake-1",
            format!("http://{addr}/v1"),
            AgentEphemeralCredential::bearer_token("sk-openai-fake-sentinel"),
        ),
    ];
    for (index, (kind, model, base_url, credential)) in routes.into_iter().enumerate() {
        let adapter = RigProviderAdapter::new(
            profile(kind.clone(), model, Some(base_url)),
            ReqwestClient::new(),
        )
        .expect("valid");
        let turn = adapter
            .call_with(
                &request_offering(
                    ASK_FOR_A_TOOL_CALL,
                    "mcp.crm.lookup",
                    AgentToolKind::RemoteMcp,
                ),
                Some(&credential),
            )
            .await
            .unwrap_or_else(|error| panic!("{kind:?} accepts every declared name: {error}"));
        assert_eq!(turn.tool_calls.len(), 1, "{kind:?}");
        assert_eq!(
            turn.tool_calls[0].tool.as_str(),
            "mcp.crm.lookup",
            "{kind:?}: the call reads back to the tool id"
        );
        assert_eq!(turn.tool_calls[0].arguments["q"], "refunds");

        // The name on the wire, a literal: `mcp_crm_lookup` plus `_` and
        // the first eight hex characters of SHA-256("mcp.crm.lookup"),
        // the name `rakka.host` declares the same tool under.
        let body = &seen.bodies.lock().expect("not poisoned")[index];
        assert_eq!(
            declared_tool_names(body),
            ["submit_result", "mcp_crm_lookup_c41f30c9"],
            "{kind:?}"
        );
    }
}

#[tokio::test]
async fn a_custom_provider_is_the_completions_shape_at_the_profiles_base_url() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let adapter = RigProviderAdapter::new(
        profile(
            AgentModelProviderKind::Custom("gateway".to_string()),
            "llama-3",
            Some(format!("http://{addr}/v1")),
        ),
        ReqwestClient::new(),
    )
    .expect("valid");
    let turn = adapter
        .call_with(
            &request("say-hello"),
            Some(&AgentEphemeralCredential::bearer_token("t")),
        )
        .await
        .expect("answers");
    assert_eq!(turn.text.as_deref(), Some("hello from the fake"));
    assert_eq!(seen.hits.load(Ordering::SeqCst), 1);

    // A custom label routes to the Chat Completions shape, not Anthropic's:
    // the body is a `messages` array with no top-level `system`, and the
    // request reached `/v1/chat/completions` (the only route that answered).
    let body = &seen.bodies.lock().expect("not poisoned")[0];
    assert!(body["messages"].is_array(), "{body}");
    assert!(
        body.get("system").is_none(),
        "the Anthropic-shaped `system` field must not appear: {body}"
    );
    assert_eq!(body["model"], "llama-3");
}

/// Azure OpenAI is the one wired provider that reads the two credential kinds
/// out of different places: an API key rides the `api-key` header and a minted
/// Entra ID / managed-identity access token rides `Authorization: Bearer`
/// (`rig-core-0.37.0/src/providers/azure.rs:143-160`). Collapsing them would
/// put an access token in the key header and earn a 401 from every Azure
/// deployment that authenticates the standard way.
#[tokio::test]
async fn azure_sends_a_bearer_token_as_a_bearer_and_an_api_key_in_the_key_header() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let attributes = BTreeMap::from([("api_version".to_string(), "2024-10-21".to_string())]);
    let adapter = RigProviderAdapter::new(
        profile_with_attributes(
            AgentModelProviderKind::AzureOpenAi,
            "gpt-fake-1",
            Some(format!("http://{addr}")),
            attributes,
        ),
        ReqwestClient::new(),
    )
    .expect("valid");

    let turn = adapter
        .call_with(
            &request("say-hello"),
            Some(&AgentEphemeralCredential::bearer_token(
                "entra-token-sentinel",
            )),
        )
        .await
        .expect("answers");
    assert_eq!(turn.text.as_deref(), Some("hello from the fake"));
    adapter
        .call_with(
            &request("say-hello"),
            Some(&AgentEphemeralCredential::api_key(
                "api-key",
                "azure-key-sentinel",
            )),
        )
        .await
        .expect("answers");

    let headers = seen.headers.lock().expect("not poisoned");
    assert_eq!(
        headers[0]
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer entra-token-sentinel"),
        "a minted access token is a bearer, not a key"
    );
    assert!(
        headers[0].get("api-key").is_none(),
        "and it is not also the key header"
    );
    assert_eq!(
        headers[1]
            .get("api-key")
            .and_then(|value| value.to_str().ok()),
        Some("azure-key-sentinel"),
        "an API key is the key header"
    );
    assert!(
        headers[1].get("authorization").is_none(),
        "and it is not also a bearer"
    );

    // Both calls reached Azure's deployment path — the only route registered
    // under it — carrying the profile attribute's API version.
    assert_eq!(seen.hits.load(Ordering::SeqCst), 2);
    let queries = seen.queries.lock().expect("not poisoned");
    assert_eq!(queries.len(), 2);
    assert_eq!(queries[0], "api-version=2024-10-21");
}

#[tokio::test]
async fn a_providers_refusal_body_never_reaches_the_durable_error() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let adapter = RigProviderAdapter::new(
        profile(
            AgentModelProviderKind::OpenAiCompletions,
            "gpt-fake-1",
            Some(format!("http://{addr}/refuse/v1")),
        ),
        ReqwestClient::new(),
    )
    .expect("valid");

    let error = adapter
        .call_with(
            &request("say-hello"),
            Some(&AgentEphemeralCredential::bearer_token("t")),
        )
        .await
        .expect_err("the provider refused");

    // Rig's own backend reads the whole 400 body into
    // `InvalidStatusCodeWithMessage` and its `Display` quotes it verbatim; a
    // real provider's 400 quotes the prompt back, and this error is persisted
    // on the outbox row and echoed onto the dispatcher fleet's index.
    let message = error.to_string();
    assert!(
        !message.contains(LEAKED_PROMPT_SENTINEL),
        "the provider's response body reached a durable error: {message}"
    );
    assert!(
        message.contains("400"),
        "the status is what the reason keeps: {message}"
    );
    assert_eq!(error.code(), "model-provider-failed");
    assert_eq!(seen.hits.load(Ordering::SeqCst), 1, "the fake did refuse");
}

#[tokio::test]
async fn a_profile_attribute_reaches_the_providers_own_header() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let attributes = BTreeMap::from([("anthropic_version".to_string(), "2023-06-01".to_string())]);
    let adapter = RigProviderAdapter::new(
        profile_with_attributes(
            AgentModelProviderKind::Anthropic,
            "claude-sonnet-5",
            Some(format!("http://{addr}")),
            attributes,
        ),
        ReqwestClient::new(),
    )
    .expect("valid");

    adapter
        .call_with(
            &request("say-hello"),
            Some(&AgentEphemeralCredential::api_key("x-api-key", "k")),
        )
        .await
        .expect("answers");

    let headers = seen.headers.lock().expect("not poisoned");
    assert_eq!(
        headers[0]
            .get("anthropic-version")
            .and_then(|value| value.to_str().ok()),
        Some("2023-06-01"),
        "the profile's non-secret attribute is what the provider was told"
    );
}

#[tokio::test]
async fn an_invalid_or_missing_base_url_is_refused_before_any_request() {
    let error = RigProviderAdapter::new(
        profile(
            AgentModelProviderKind::Anthropic,
            "m",
            Some("https://u:p@host/v1".to_string()),
        ),
        ReqwestClient::new(),
    )
    .expect_err("refused at construction");
    assert_eq!(error.code(), "model-profile-invalid-base-url");
    let error = RigProviderAdapter::new(
        profile(AgentModelProviderKind::Custom("x".to_string()), "m", None),
        ReqwestClient::new(),
    )
    .expect_err("a custom provider needs a base url");
    assert_eq!(error.code(), "model-profile-invalid-base-url");
}

#[tokio::test]
async fn unsupported_or_missing_credential_material_is_refused_before_any_request() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let adapter = RigProviderAdapter::new(
        profile(
            AgentModelProviderKind::Anthropic,
            "m",
            Some(format!("http://{addr}")),
        ),
        ReqwestClient::new(),
    )
    .expect("valid");
    let basic = AgentEphemeralCredential::basic("user", "pw");
    let error = adapter
        .call_with(&request("say-hello"), Some(&basic))
        .await
        .expect_err("basic is not a provider key");
    assert_eq!(error.code(), "model-credential-material-unsupported");
    // The refusal names the material kind and the provider, never a value.
    let message = error.to_string();
    assert!(message.contains("basic"), "{message}");
    assert!(
        !message.contains("user") && !message.contains("pw"),
        "no credential value in a durable error: {message}"
    );
    let error = adapter
        .call_with(&request("say-hello"), None)
        .await
        .expect_err("anthropic needs a key");
    assert_eq!(error.code(), "model-credential-missing");
    assert_eq!(
        seen.hits.load(Ordering::SeqCst),
        0,
        "nothing reached the fake"
    );
}

/// Every send goes through the injected backend: a counting wrapper around
/// rig's own re-exported `reqwest` client, implementing `HttpClientExt` by
/// delegation. Naming it through `rig_core` rather than a direct `reqwest`
/// dev-dependency is what keeps this compiling: the blanket impl exists only
/// for the exact `reqwest` rig links, and a dev-dependency free to resolve to
/// a different one would silently stop satisfying the bound.
#[derive(Clone, Default, Debug)]
struct Counting {
    inner: ReqwestClient,
    sends: Arc<AtomicUsize>,
}

impl HttpClientExt for Counting {
    fn send<T, U>(
        &self,
        req: http_client::Request<T>,
    ) -> impl Future<Output = http_client::Result<http_client::Response<LazyBody<U>>>>
           + WasmCompatSend
           + 'static
    where
        T: Into<Bytes>,
        T: WasmCompatSend,
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.inner.send(req)
    }

    fn send_multipart<U>(
        &self,
        req: http_client::Request<MultipartForm>,
    ) -> impl Future<Output = http_client::Result<http_client::Response<LazyBody<U>>>>
           + WasmCompatSend
           + 'static
    where
        U: From<Bytes>,
        U: WasmCompatSend + 'static,
    {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.inner.send_multipart(req)
    }

    fn send_streaming<T>(
        &self,
        req: http_client::Request<T>,
    ) -> impl Future<Output = http_client::Result<StreamingResponse>> + WasmCompatSend
    where
        T: Into<Bytes> + WasmCompatSend,
    {
        self.sends.fetch_add(1, Ordering::SeqCst);
        self.inner.send_streaming(req)
    }
}

#[tokio::test]
async fn every_send_goes_through_the_injected_backend() {
    let seen = Seen::default();
    let addr = serve(seen.clone()).await;
    let counting = Counting::default();
    let adapter = RigProviderAdapter::new(
        profile(
            AgentModelProviderKind::OpenAiCompletions,
            "gpt-fake-1",
            Some(format!("http://{addr}/v1")),
        ),
        counting.clone(),
    )
    .expect("valid");
    adapter
        .call_with(
            &request("say-hello"),
            Some(&AgentEphemeralCredential::bearer_token("t")),
        )
        .await
        .expect("answers");
    assert_eq!(counting.sends.load(Ordering::SeqCst), 1);
    assert_eq!(seen.hits.load(Ordering::SeqCst), 1);
}
