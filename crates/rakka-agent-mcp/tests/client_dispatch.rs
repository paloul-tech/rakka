//! The MCP executor per attempt: the egress check before any client, the
//! credential on the wire and nowhere else, `_meta` with the idempotency
//! key and trace context, the schema recheck, and every result mapping —
//! against the in-process fake, over a counting client.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use std::sync::Arc;

use rakka_agent::{
    AgentAuthorityRefusal, AgentDispatchToolExecutor, AgentEffectSafetyClass, AgentTaskContent,
    AgentToolCallId, AgentToolCallRequest, AgentToolDeclaration, AgentToolId,
    AgentToolResultBehavior,
};
use rakka_agent_mcp::testkit::{
    serve_fake, CountingClient, FakeMcpServer, FakeTool, FakeToolBehaviour, ReqwestClient,
};
use rakka_agent_mcp::{
    mcp_artifact_store, sync_mcp_descriptors, McpAllowAllEgress, McpDispatchToolExecutor,
    McpEgressCheck, McpServerBinding, McpServerId, McpToolPolicy, MCP_META_IDEMPOTENCY_KEY,
};
use rakka_agent_workflow::{AgentEphemeralCredential, AgentTimestampMillis};
use serde_json::json;

mod support;
use support::{run_scope, tool_intent_with_timeout, SharedArtifactStore};

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
        .with_credential_binding(
            rakka_agent::AgentCredentialBindingRef::new("crm-key").expect("binding"),
        )
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
    let http = CountingClient::new();
    let binding = binding(url);
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
    let (executor, http, _) = executor(&endpoint.url, Arc::new(McpAllowAllEgress)).await;
    let sends_after_sync = http.sends();
    let credential = AgentEphemeralCredential::bearer_token("attempt-token-sentinel");
    let intent = tool_intent_with_timeout("mcp.crm.echo", Some(5_000));
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
    assert!(
        seen[0].meta.get("traceparent").is_some() == intent.telemetry.trace_parent.is_some(),
        "the trace context rides `_meta` exactly when the effect carries one"
    );
    assert!(
        http.sends() > sends_after_sync,
        "every send went through the injected client"
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
    endpoint.server.swap_tool_schema(
        "echo",
        json!({"type":"object","properties":{"changed":{"type":"string"}}}),
    );
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
        endpoint.server.call_count(),
        2,
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
    assert_eq!(store.len().await, 1);
    assert!(store
        .bytes(&reference.artifact_id)
        .await
        .is_some_and(|b| b.len() > 5000));
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
    let (executor, http, _) = executor(&endpoint.url, Arc::new(RefuseAll)).await;
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
    let started = std::time::Instant::now();
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
        started.elapsed() < std::time::Duration::from_secs(2),
        "the timeout bounded the call"
    );
    assert!(
        error.to_string().contains("mcp-transport-failed"),
        "{error}"
    );
}
