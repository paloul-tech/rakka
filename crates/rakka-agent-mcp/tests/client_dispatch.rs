//! The MCP executor per attempt: the egress check before any client, the
//! credential on the wire and nowhere else, `_meta` with the idempotency
//! key and trace context, the schema recheck, every result mapping, and the
//! effect's timeout over the whole attempt — against the in-process fake,
//! over a counting client.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use rakka_agent::{
    AgentAuthorityRefusal, AgentContentDigest, AgentDispatchToolExecutor, AgentEffectSafetyClass,
    AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolDeclaration, AgentToolId,
    AgentToolResultBehavior,
};
use rakka_agent_mcp::testkit::{
    serve_fake, CountingClient, FakeMcpServer, FakeTool, FakeToolBehaviour, ReqwestClient,
};
use rakka_agent_mcp::{
    mcp_artifact_store, sync_mcp_descriptors, McpAllowAllEgress, McpDescriptorSet,
    McpDispatchToolExecutor, McpEgressCheck, McpRegistrationError, McpServerBinding, McpServerId,
    McpToolPolicy, MCP_LIST_PAGES_MAX, MCP_META_IDEMPOTENCY_KEY,
};
use rakka_agent_workflow::{
    validate_artifact_ref, AgentEphemeralCredential, AgentTimestampMillis,
    DEFAULT_AGENT_ARTIFACT_RETENTION_CLASS,
};
use serde_json::json;

mod support;
use support::{run_scope, tool_intent_with_timeout, SharedArtifactStore};

/// A W3C `traceparent` the effect carries, as a real run would commit one.
const TRACE_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-00f067aa0ba902b7-01";
/// A W3C `tracestate` the effect carries alongside it.
const TRACE_STATE: &str = "rakka=t61rcWkgMzE";

/// What every elapsed deadline reads as, wherever in the attempt it fired.
const ATTEMPT_TIMED_OUT: &str = "the attempt exceeded the effect's timeout";

fn server_id() -> McpServerId {
    McpServerId::new("crm").expect("id")
}

fn policy(class: AgentEffectSafetyClass, behavior: AgentToolResultBehavior) -> McpToolPolicy {
    McpToolPolicy::new(AgentToolDeclaration::new(class)).with_result_behavior(behavior)
}

fn fake() -> FakeMcpServer {
    FakeMcpServer::new()
        .with_tool(FakeTool::new(
            "echo",
            "Echoes.",
            json!({"type":"object"}),
            FakeToolBehaviour::Echo,
        ))
        .with_tool(FakeTool::new(
            "big",
            "Big.",
            json!({"type":"object"}),
            FakeToolBehaviour::Text("x".repeat(5000)),
        ))
        .with_tool(FakeTool::new(
            "img",
            "Image.",
            json!({"type":"object"}),
            FakeToolBehaviour::Image { bytes: 16 },
        ))
        .with_tool(FakeTool::new(
            "fail",
            "Fails.",
            json!({"type":"object"}),
            FakeToolBehaviour::Error("boom\nline2 with-arg-echo".into()),
        ))
        .with_tool(FakeTool::new(
            "ask",
            "Asks.",
            json!({"type":"object"}),
            FakeToolBehaviour::InputRequired,
        ))
}

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(server_id(), url)
        .with_tool(
            "echo",
            policy(
                AgentEffectSafetyClass::ReadOnly,
                AgentToolResultBehavior::InlineBounded,
            ),
        )
        .expect("t")
        .with_tool(
            "big",
            policy(
                AgentEffectSafetyClass::ReadOnly,
                AgentToolResultBehavior::ArtifactReference,
            ),
        )
        .expect("t")
        .with_tool(
            "img",
            policy(
                AgentEffectSafetyClass::ReadOnly,
                AgentToolResultBehavior::InlineBounded,
            ),
        )
        .expect("t")
        .with_tool(
            "fail",
            policy(
                AgentEffectSafetyClass::ReadOnly,
                AgentToolResultBehavior::InlineBounded,
            ),
        )
        .expect("t")
        .with_tool(
            "ask",
            policy(
                AgentEffectSafetyClass::ReadOnly,
                AgentToolResultBehavior::InlineBounded,
            ),
        )
        .expect("t")
}

/// The logical credential binding a credentialed proof names.
fn credential_binding() -> rakka_agent::AgentCredentialBindingRef {
    rakka_agent::AgentCredentialBindingRef::new("crm-key").expect("binding")
}

fn call(tool: &str, arguments: serde_json::Value) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new("call-1").expect("id"),
        AgentToolId::new(format!("mcp.crm.{tool}")).expect("id"),
        arguments,
    )
    .expect("call")
}

async fn executor(
    url: &str,
    egress: Arc<dyn McpEgressCheck>,
) -> (
    McpDispatchToolExecutor<CountingClient>,
    CountingClient,
    SharedArtifactStore,
) {
    executor_over(binding(url), egress).await
}

/// As [`executor`], over the given binding.
async fn executor_over(
    binding: McpServerBinding,
    egress: Arc<dyn McpEgressCheck>,
) -> (
    McpDispatchToolExecutor<CountingClient>,
    CountingClient,
    SharedArtifactStore,
) {
    let http = CountingClient::new();
    let set = sync_mcp_descriptors(
        &http,
        &binding,
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    let store = SharedArtifactStore::default();
    let executor = McpDispatchToolExecutor::new(
        vec![set],
        vec![binding],
        mcp_artifact_store(store.clone()),
        http.clone(),
        egress,
    )
    .expect("builds");
    (executor, http, store)
}

#[tokio::test]
async fn a_call_carries_the_credential_the_meta_and_maps_structured_content_inline() {
    let endpoint = serve_fake(fake()).await;
    let (executor, http, _) = executor_over(
        binding(&endpoint.url).with_credential_binding(credential_binding()),
        Arc::new(McpAllowAllEgress),
    )
    .await;
    let sends_after_sync = http.sends();
    let credential = AgentEphemeralCredential::bearer_token("attempt-token-sentinel");
    let mut intent = tool_intent_with_timeout("mcp.crm.echo", Some(5_000));
    intent.telemetry.trace_parent = Some(TRACE_PARENT.to_string());
    intent.telemetry.trace_state = Some(TRACE_STATE.to_string());
    let content = executor
        .execute(
            &run_scope(),
            &intent,
            &call("echo", json!({"q": "refunds"})),
            Some(&credential),
        )
        .await
        .expect("answers");
    assert!(
        matches!(content, AgentTaskContent::Inline(ref v) if v["q"] == "refunds"),
        "{content:?}"
    );
    let seen = endpoint.server.seen_calls();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].name, "echo");
    assert!(seen[0]
        .authorization
        .as_deref()
        .is_some_and(|v| v.ends_with("attempt-token-sentinel")));
    assert_eq!(
        seen[0].meta[MCP_META_IDEMPOTENCY_KEY],
        json!(intent.idempotency_key.as_str())
    );
    assert_eq!(
        seen[0].meta["traceparent"],
        json!(TRACE_PARENT),
        "the effect's trace parent rides `_meta`: {:?}",
        seen[0].meta
    );
    assert_eq!(
        seen[0].meta["tracestate"],
        json!(TRACE_STATE),
        "the effect's trace state rides `_meta`: {:?}",
        seen[0].meta
    );
    // Every send went through the injected client, and the attempt made
    // exactly three: the `server/discover` handshake, the recheck's one-page
    // `tools/list` (the first attempt finds the cache empty), and the
    // `tools/call`. A discover-negotiated session is stateless — no session
    // id, so no standalone stream to open and no session to delete at close.
    assert_eq!(
        http.sends() - sends_after_sync,
        3,
        "one attempt: handshake, recheck, call"
    );
    assert_eq!(
        endpoint.server.list_calls(),
        2,
        "one at sync, one recheck on the first attempt"
    );
}

#[tokio::test]
async fn the_recheck_is_cached_under_the_ttl_and_refuses_a_changed_schema() {
    let endpoint = serve_fake(fake()).await;
    let (executor, _, _) = executor(&endpoint.url, Arc::new(McpAllowAllEgress)).await;
    let intent = tool_intent_with_timeout("mcp.crm.echo", Some(5_000));
    executor
        .execute(&run_scope(), &intent, &call("echo", json!({})), None)
        .await
        .expect("first");
    executor
        .execute(&run_scope(), &intent, &call("echo", json!({})), None)
        .await
        .expect("second");
    assert_eq!(
        endpoint.server.list_calls(),
        2,
        "the second attempt reused the cached listing"
    );
    let every_attempt = {
        let http = ReqwestClient::new();
        let binding = binding(&endpoint.url);
        let set = sync_mcp_descriptors(
            &http,
            &binding,
            None,
            AgentTimestampMillis::new(1),
            &McpAllowAllEgress,
        )
        .await
        .expect("syncs");
        McpDispatchToolExecutor::new(
            vec![set],
            vec![binding],
            mcp_artifact_store(SharedArtifactStore::default()),
            http,
            Arc::new(McpAllowAllEgress),
        )
        .expect("builds")
        .with_descriptor_recheck_ttl_ms(0)
    };
    // Two successful attempts on the TTL-0 executor: the first would list on
    // an empty cache whatever the TTL said, so only the second one shows that
    // `0` means "every attempt".
    let listed_before = endpoint.server.list_calls();
    for attempt in ["first", "second"] {
        every_attempt
            .execute(&run_scope(), &intent, &call("echo", json!({})), None)
            .await
            .unwrap_or_else(|error| panic!("the {attempt} TTL-0 attempt answers: {error}"));
    }
    assert_eq!(
        endpoint.server.list_calls(),
        listed_before + 2,
        "a TTL of 0 re-reads the listing on every attempt"
    );
    endpoint.server.swap_tool_schema(
        "echo",
        json!({"type":"object","properties":{"changed":{"type":"string"}}}),
    );
    // The default-TTL executor still holds a listing from before the swap, so
    // it answers from the cache — that is what a TTL costs — and lists
    // nothing.
    executor
        .execute(&run_scope(), &intent, &call("echo", json!({})), None)
        .await
        .expect("a cache hit compares against the last listing it read");
    assert_eq!(
        endpoint.server.list_calls(),
        listed_before + 2,
        "the cache hit made no listing"
    );
    let calls_before_mismatch = endpoint.server.call_count();
    assert_eq!(calls_before_mismatch, 5, "2 + 2 TTL-0 + 1 cache hit");
    let error = every_attempt
        .execute(&run_scope(), &intent, &call("echo", json!({})), None)
        .await
        .expect_err("mismatch");
    assert!(
        error
            .to_string()
            .contains("tool-descriptor-revision-mismatch"),
        "{error}"
    );
    assert_eq!(
        endpoint.server.list_calls(),
        listed_before + 3,
        "the TTL-0 executor re-read the reshaped listing"
    );
    assert_eq!(
        endpoint.server.call_count(),
        calls_before_mismatch,
        "the mismatched attempt never called the tool"
    );
}

#[tokio::test]
async fn large_results_go_to_the_artifact_store_or_refuse_by_the_bindings_behavior() {
    let endpoint = serve_fake(fake()).await;
    let (executor, _, store) = executor(&endpoint.url, Arc::new(McpAllowAllEgress)).await;
    let intent = tool_intent_with_timeout("mcp.crm.big", Some(5_000));
    let content = executor
        .execute(&run_scope(), &intent, &call("big", json!({})), None)
        .await
        .expect("artifact");
    let AgentTaskContent::Artifact(reference) = content else {
        panic!("expected an artifact, got {content:?}")
    };
    // The fixture store passes the writer's checksum through and invents
    // none, so a reference that validates here is one whose checksum the
    // executor itself stamped.
    validate_artifact_ref(&reference).unwrap_or_else(|error| {
        panic!("the stored reference passes the workflow's own validation: {error}")
    });
    assert_eq!(
        reference.retention_class.as_deref(),
        Some(DEFAULT_AGENT_ARTIFACT_RETENTION_CLASS),
        "the write request kept the crate's default retention class"
    );
    assert_eq!(
        reference.artifact_id,
        format!(
            "mcp-{}-g{}-call-1",
            intent.effect_id,
            intent.generation.get()
        ),
        "the artifact id derives from the effect, its generation, and the call"
    );
    assert_eq!(store.len().await, 1);
    let stored = store
        .bytes(&reference.artifact_id)
        .await
        .expect("the result reached the store");
    assert!(stored.len() > 5000);
    assert_eq!(
        reference.checksum,
        Some(format!(
            "sha256:{}",
            AgentContentDigest::sha256_of_bytes(&stored).value
        )),
        "the executor stamps SHA-256 over the exact bytes it wrote"
    );
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.img", Some(5_000)),
            &call("img", json!({})),
            None,
        )
        .await
        .expect_err("inline binding, binary part");
    assert!(
        error.to_string().contains("mcp-result-too-large"),
        "{error}"
    );
}

#[tokio::test]
async fn is_error_and_input_required_are_stable_refusals_with_bounded_body_free_detail() {
    let endpoint = serve_fake(fake()).await;
    let (executor, _, _) = executor(&endpoint.url, Arc::new(McpAllowAllEgress)).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.fail", Some(5_000)),
            &call("fail", json!({"secret_arg": "ARG-SENTINEL"})),
            None,
        )
        .await
        .expect_err("isError");
    let text = error.to_string();
    assert!(
        text.contains("mcp-tool-error") && text.contains("boom line2"),
        "{text}"
    );
    assert!(
        !text.contains("ARG-SENTINEL") && !text.contains('\n'),
        "{text}"
    );
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.ask", Some(5_000)),
            &call("ask", json!({})),
            None,
        )
        .await
        .expect_err("MRTR");
    assert!(error.to_string().contains("mcp-input-required"), "{error}");
}

struct RefuseAll;

impl McpEgressCheck for RefuseAll {
    fn check(&self, server: &McpServerId, _url: &str) -> Result<(), AgentAuthorityRefusal> {
        Err(AgentAuthorityRefusal::of(
            "egress-denied-by-policy",
            format!("{server} is not an allowed egress"),
        ))
    }
}

#[tokio::test]
async fn an_egress_refusal_fails_before_any_client_exists_and_never_touches_the_credential() {
    let endpoint = serve_fake(fake()).await;
    let (executor, http, _) = executor_over(
        binding(&endpoint.url).with_credential_binding(credential_binding()),
        Arc::new(RefuseAll),
    )
    .await;
    let sends_after_sync = http.sends();
    let credential = AgentEphemeralCredential::bearer_token("never-sent");
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.echo", Some(5_000)),
            &call("echo", json!({})),
            Some(&credential),
        )
        .await
        .expect_err("refused");
    assert!(
        error.to_string().contains("egress-denied-by-policy"),
        "the deployment's own code: {error}"
    );
    assert_eq!(http.sends(), sends_after_sync, "no client was built");
    assert!(endpoint.server.seen_calls().is_empty());
}

#[tokio::test]
async fn an_http_binding_that_names_a_credential_fails_closed_without_one() {
    let endpoint = serve_fake(fake()).await;
    let mut declaration = AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly);
    declaration.credential_binding = Some(credential_binding());
    // The server's binding alone, and a tool's declaration alone: either one
    // names a credential the attempt must carry.
    let server_level = McpServerBinding::streamable_http(server_id(), &endpoint.url)
        .with_tool(
            "echo",
            policy(
                AgentEffectSafetyClass::ReadOnly,
                AgentToolResultBehavior::InlineBounded,
            ),
        )
        .expect("t")
        .with_credential_binding(credential_binding());
    let tool_level = McpServerBinding::streamable_http(server_id(), &endpoint.url)
        .with_tool("echo", McpToolPolicy::new(declaration))
        .expect("t");
    for (level, binding) in [("server", server_level), ("tool", tool_level)] {
        let (executor, http, _) = executor_over(binding, Arc::new(McpAllowAllEgress)).await;
        let sends_after_sync = http.sends();
        let error = executor
            .execute(
                &run_scope(),
                &tool_intent_with_timeout("mcp.crm.echo", Some(5_000)),
                &call("echo", json!({})),
                None,
            )
            .await
            .expect_err("a named credential that did not arrive");
        let text = error.to_string();
        assert!(
            text.contains("mcp-credential-missing") && text.contains("crm-key"),
            "the {level}-level binding: {text}"
        );
        assert_eq!(
            http.sends(),
            sends_after_sync,
            "the {level}-level attempt sent nothing without its credential"
        );
    }
    assert_eq!(
        endpoint.server.call_count(),
        0,
        "no call reached the server"
    );
}

/// A credential value a hostile server echoes back into what it answers.
const ECHOED_TOKEN: &str = "echoed-token-sentinel";

#[tokio::test]
async fn server_chosen_text_is_scrubbed_of_the_attempts_credential() {
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(FakeTool::new(
                "leak",
                "Fails, quoting the caller's token.",
                json!({"type":"object"}),
                FakeToolBehaviour::Error(format!("denied: {ECHOED_TOKEN} is revoked\nretry")),
            ))
            .with_tool(FakeTool::new(
                "reflect",
                "Answers with the caller's token.",
                json!({"type":"object"}),
                FakeToolBehaviour::Text(format!("token={ECHOED_TOKEN}")),
            )),
    )
    .await;
    let binding = McpServerBinding::streamable_http(server_id(), &endpoint.url)
        .with_tool(
            "leak",
            policy(
                AgentEffectSafetyClass::ReadOnly,
                AgentToolResultBehavior::InlineBounded,
            ),
        )
        .expect("t")
        .with_tool(
            "reflect",
            policy(
                AgentEffectSafetyClass::ReadOnly,
                AgentToolResultBehavior::InlineBounded,
            ),
        )
        .expect("t")
        .with_credential_binding(credential_binding());
    let (executor, _, _) = executor_over(binding, Arc::new(McpAllowAllEgress)).await;

    // The tool's own error text, under either material kind the server can
    // receive: the value is gone, the rest of the line is kept.
    for credential in [
        AgentEphemeralCredential::bearer_token(ECHOED_TOKEN),
        AgentEphemeralCredential::api_key("x-api-key", ECHOED_TOKEN),
    ] {
        let error = executor
            .execute(
                &run_scope(),
                &tool_intent_with_timeout("mcp.crm.leak", Some(5_000)),
                &call("leak", json!({})),
                Some(&credential),
            )
            .await
            .expect_err("isError");
        let text = error.to_string();
        assert!(
            text.contains("mcp-tool-error") && text.contains("denied: <redacted> is revoked retry"),
            "{text}"
        );
        assert!(!text.contains(ECHOED_TOKEN), "{text}");
    }

    // A successful answer is kept content too.
    let content = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.reflect", Some(5_000)),
            &call("reflect", json!({})),
            Some(&AgentEphemeralCredential::bearer_token(ECHOED_TOKEN)),
        )
        .await
        .expect("answers");
    assert_eq!(
        content,
        AgentTaskContent::inline(json!({ "text": "token=<redacted>" })).expect("inline")
    );

    // Positive control: with no credential there is nothing to scrub, and the
    // same answer reads back verbatim.
    let (unbound, _, _) = executor_over(
        McpServerBinding::streamable_http(server_id(), &endpoint.url)
            .with_tool(
                "reflect",
                policy(
                    AgentEffectSafetyClass::ReadOnly,
                    AgentToolResultBehavior::InlineBounded,
                ),
            )
            .expect("t"),
        Arc::new(McpAllowAllEgress),
    )
    .await;
    let verbatim = unbound
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.reflect", Some(5_000)),
            &call("reflect", json!({})),
            None,
        )
        .await
        .expect("answers");
    assert_eq!(
        verbatim,
        AgentTaskContent::inline(json!({ "text": format!("token={ECHOED_TOKEN}") }))
            .expect("inline")
    );
}

#[tokio::test]
async fn a_server_that_identifies_as_a_rakka_agent_is_refused_at_dispatch_as_well() {
    // The publish-time sync saw a healthy server; by dispatch time the URL
    // answers as a Rakka agent — and one that folds the caller's token into
    // the name it reports.
    let impostor = serve_fake(
        FakeMcpServer::new()
            .with_tool(echo_tool())
            .with_server_name(format!("rakka-agent-{ECHOED_TOKEN}")),
    )
    .await;
    let executor = echo_executor_at(&impostor.url).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.echo", Some(5_000)),
            &call("echo", json!({})),
            Some(&AgentEphemeralCredential::bearer_token(ECHOED_TOKEN)),
        )
        .await
        .expect_err("MCP is never an agent-to-agent channel");
    let text = error.to_string();
    assert!(
        text.contains("mcp-peer-agent-channel-refused") && text.contains("rakka-agent-<redacted>"),
        "{text}"
    );
    assert!(!text.contains(ECHOED_TOKEN), "{text}");
    assert_eq!(
        impostor.server.list_calls(),
        0,
        "the refused session listed nothing"
    );
    assert_eq!(impostor.server.call_count(), 0, "and called nothing");
}

#[tokio::test]
async fn a_listing_that_never_ends_is_refused_at_the_page_cap_by_the_sync_and_the_recheck() {
    let endless = serve_fake(
        FakeMcpServer::new()
            .with_tool(echo_tool())
            .with_endless_pages(),
    )
    .await;
    let error = sync_mcp_descriptors(
        &ReqwestClient::new(),
        &echo_binding(&endless.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect_err("the sync never reads the listing to its end");
    assert_eq!(error.code(), "mcp-descriptor-sync-failed");
    assert!(
        error
            .to_string()
            .contains(&format!("{MCP_LIST_PAGES_MAX} pages")),
        "{error}"
    );
    assert_eq!(endless.server.list_calls(), MCP_LIST_PAGES_MAX);

    // The recheck: the set was synced from a healthy server, and the endless
    // one answers at dispatch time.
    let executor = echo_executor_at(&endless.url).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.echo", Some(5_000)),
            &call("echo", json!({})),
            None,
        )
        .await
        .expect_err("the recheck confirms nothing it did not read to its end");
    assert!(
        error
            .to_string()
            .contains("tool-descriptor-revision-mismatch"),
        "{error}"
    );
    assert_eq!(endless.server.list_calls(), 2 * MCP_LIST_PAGES_MAX);
    assert_eq!(endless.server.call_count(), 0, "the tool was never called");
}

#[tokio::test]
async fn construction_is_offline_and_a_tool_without_a_descriptor_is_refused() {
    let endpoint = serve_fake(fake()).await;
    let http = ReqwestClient::new();
    let binding = binding(&endpoint.url);
    let set = sync_mcp_descriptors(
        &http,
        &binding,
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    let lists = endpoint.server.list_calls();
    let executor = McpDispatchToolExecutor::new(
        vec![set.clone()],
        vec![binding.clone()],
        mcp_artifact_store(SharedArtifactStore::default()),
        http.clone(),
        Arc::new(McpAllowAllEgress),
    )
    .expect("builds");
    assert_eq!(
        endpoint.server.list_calls(),
        lists,
        "construction makes no network call"
    );
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.nope", Some(1_000)),
            &call("nope", json!({})),
            None,
        )
        .await
        .expect_err("unbound");
    assert!(error.to_string().contains("mcp-tool-unbound"), "{error}");
    let widened = binding
        .with_tool(
            "extra",
            policy(
                AgentEffectSafetyClass::ReadOnly,
                AgentToolResultBehavior::InlineBounded,
            ),
        )
        .expect("t");
    let error = McpDispatchToolExecutor::new(
        vec![set],
        vec![widened],
        mcp_artifact_store(SharedArtifactStore::default()),
        http,
        Arc::new(McpAllowAllEgress),
    )
    .expect_err("a listed tool with no synced descriptor");
    assert_eq!(error.code(), "mcp-binding-invalid");
    assert!(
        matches!(
            error,
            McpRegistrationError::DescriptorMissing { ref server, ref tool }
                if server == "crm" && tool == "extra"
        ),
        "{error:?}"
    );
    assert_eq!(
        error.to_string(),
        "the MCP server crm's tool extra has no synced descriptor; re-sync the server before \
         binding it"
    );
}

#[tokio::test]
async fn a_timeout_from_the_intent_bounds_the_call() {
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(FakeTool::new(
        "slow",
        "Slow.",
        json!({"type":"object"}),
        FakeToolBehaviour::Sleep { millis: 2_000 },
    )))
    .await;
    let http = ReqwestClient::new();
    let binding = McpServerBinding::streamable_http(server_id(), &endpoint.url)
        .with_tool(
            "slow",
            policy(
                AgentEffectSafetyClass::ReadOnly,
                AgentToolResultBehavior::InlineBounded,
            ),
        )
        .expect("t");
    let set = sync_mcp_descriptors(
        &http,
        &binding,
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    let executor = McpDispatchToolExecutor::new(
        vec![set],
        vec![binding],
        mcp_artifact_store(SharedArtifactStore::default()),
        http,
        Arc::new(McpAllowAllEgress),
    )
    .expect("builds");
    let started = Instant::now();
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.slow", Some(200)),
            &call("slow", json!({})),
            None,
        )
        .await
        .expect_err("timed out");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the timeout bounded the call"
    );
    assert!(
        error.to_string().contains("mcp-transport-failed")
            && error.to_string().contains(ATTEMPT_TIMED_OUT),
        "{error}"
    );
}

/// The echo tool alone, bound on `url`.
fn echo_binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(server_id(), url)
        .with_tool(
            "echo",
            policy(
                AgentEffectSafetyClass::ReadOnly,
                AgentToolResultBehavior::InlineBounded,
            ),
        )
        .expect("t")
}

fn echo_tool() -> FakeTool {
    FakeTool::new(
        "echo",
        "Echoes.",
        json!({"type":"object"}),
        FakeToolBehaviour::Echo,
    )
}

/// The descriptor set a publish-time sync reads from a healthy server
/// exposing only the echo tool.
async fn echo_set_from_a_healthy_server() -> McpDescriptorSet {
    let healthy = serve_fake(FakeMcpServer::new().with_tool(echo_tool())).await;
    sync_mcp_descriptors(
        &ReqwestClient::new(),
        &echo_binding(&healthy.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs")
}

/// An executor whose only binding points at `url`, over a set synced from a
/// healthy server — the publish-time sync saw a server that answered; the
/// dispatch-time one is whatever `url` serves.
async fn echo_executor_at(url: &str) -> McpDispatchToolExecutor<ReqwestClient> {
    McpDispatchToolExecutor::new(
        vec![echo_set_from_a_healthy_server().await],
        vec![echo_binding(url)],
        mcp_artifact_store(SharedArtifactStore::default()),
        ReqwestClient::new(),
        Arc::new(McpAllowAllEgress),
    )
    .expect("builds")
}

#[tokio::test]
async fn the_intents_timeout_bounds_a_listing_that_stalls_before_the_call() {
    // The same tool, served by a server that opens the session and answers
    // the handshake, then sits on its `tools/list` for far longer than the
    // effect allows.
    let stalled = serve_fake(
        FakeMcpServer::new()
            .with_tool(echo_tool())
            .with_list_delay(2_000),
    )
    .await;
    let executor = echo_executor_at(&stalled.url).await;
    let started = Instant::now();
    // The outer bound only keeps a regression from hanging the suite; the
    // assertion below is the one that measures the fix.
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        executor.execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.echo", Some(200)),
            &call("echo", json!({})),
            None,
        ),
    )
    .await
    .expect("the attempt returned on its own")
    .expect_err("the listing stalled past the effect's timeout");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the deadline fired inside the stalled listing, not after it: {:?}",
        started.elapsed()
    );
    assert!(
        error.to_string().contains("mcp-transport-failed")
            && error.to_string().contains(ATTEMPT_TIMED_OUT),
        "{error}"
    );
    assert_eq!(
        stalled.server.list_calls(),
        1,
        "the attempt reached the recheck's listing"
    );
    assert_eq!(
        stalled.server.call_count(),
        0,
        "the stalled attempt never sent tools/call"
    );
}

/// A socket that accepts every connection and never writes a byte: an
/// `initialize` or `server/discover` request goes out and no answer ever comes
/// back. Returns the endpoint URL and the task holding the connections open.
async fn silent_endpoint() -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the loopback socket binds");
    let address = listener.local_addr().expect("the address is readable");
    let silent = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });
    (format!("http://{address}/mcp"), silent)
}

#[tokio::test]
async fn an_intent_with_no_timeout_is_still_bounded_by_the_executors_default() {
    // The effect committed no timeout at all — a decoded or hand-built policy
    // with `timeout_ms: None` — and the server never answers. The executor's
    // own default bounds the attempt anyway; the test shortens it, since the
    // crate's default is thirty seconds.
    let (url, silent) = silent_endpoint().await;
    let executor = echo_executor_at(&url)
        .await
        .with_attempt_timeout_default_ms(200);
    let intent = tool_intent_with_timeout("mcp.crm.echo", None);
    assert_eq!(intent.timeout_ms, None, "the effect carries no timeout");
    let started = Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        executor.execute(&run_scope(), &intent, &call("echo", json!({})), None),
    )
    .await
    .expect("the attempt returned on its own rather than waiting forever")
    .expect_err("the handshake was never answered");
    silent.abort();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the executor's default fired inside the handshake: {:?}",
        started.elapsed()
    );
    assert!(
        error.to_string().contains("mcp-transport-failed")
            && error.to_string().contains(ATTEMPT_TIMED_OUT),
        "{error}"
    );
}

#[tokio::test]
async fn the_intents_timeout_bounds_a_handshake_that_is_never_answered() {
    let (url, silent) = silent_endpoint().await;
    let executor = echo_executor_at(&url).await;
    let credential = AgentEphemeralCredential::bearer_token("attempt-token-sentinel");
    let started = Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        executor.execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.echo", Some(200)),
            &call("echo", json!({})),
            Some(&credential),
        ),
    )
    .await
    .expect("the attempt returned on its own")
    .expect_err("the handshake was never answered");
    silent.abort();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the deadline fired inside the handshake: {:?}",
        started.elapsed()
    );
    assert!(
        error.to_string().contains("mcp-transport-failed")
            && error.to_string().contains(ATTEMPT_TIMED_OUT),
        "{error}"
    );
}
