//! The executor's artifact write path: a sink that is not a pass-through
//! store, the run scope it is handed, the reference it returns, and the
//! 2 KiB bound measured as the run itself measures it.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use std::sync::{Arc, Mutex};

use rakka_agent::{
    AgentContentDigest, AgentDispatchToolExecutor, AgentEffectSafetyClass, AgentRunScope,
    AgentTaskContent, AgentToolCallId, AgentToolCallRequest, AgentToolDeclaration, AgentToolId,
    AgentToolResultBehavior, AGENT_TOOL_RESULT_MAX_BYTES,
};
use rakka_agent_mcp::testkit::{
    serve_fake, CountingClient, FakeMcpServer, FakeTool, FakeToolBehaviour,
};
use rakka_agent_mcp::{
    mcp_artifact_store, sync_mcp_descriptors, McpAllowAllEgress, McpArtifactFuture,
    McpArtifactSink, McpArtifacts, McpDispatchToolExecutor, McpServerBinding, McpServerId,
    McpToolPolicy, MCP_INLINE_RESULT_MAX_BYTES,
};
use rakka_agent_workflow::{
    AgentArtifactError, AgentArtifactWriteRequest, AgentAttributes, AgentEphemeralCredential,
    AgentTimestampMillis, ArtifactKind, ArtifactRef, RedactionStatus,
};
use serde_json::json;

mod support;
use support::{run_scope, tool_intent_with_timeout, SharedArtifactStore};

/// A credential value the fake server echoes into a large result.
const ECHOED_TOKEN: &str = "sink-token-sentinel";

/// How the recording sink answers a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkMode {
    /// A content-addressed store: the SHA-256 of the bytes is the id, and the
    /// requested id is ignored.
    ContentAddressed,
    /// As above, but the reference says `redaction: Unknown`, which the
    /// default artifact policy refuses.
    UnknownRedaction,
    /// The store is down.
    Failing,
    /// A valid reference whose URI alone is larger than the run's bound.
    HugeUri,
}

/// One write, as the sink saw it.
#[derive(Debug, Clone)]
struct SeenWrite {
    scope_key: String,
    request: AgentArtifactWriteRequest,
}

/// A `&self` sink, as a deployment's own store is: it records what it was
/// handed and mints its own reference.
#[derive(Debug, Clone)]
struct RecordingSink {
    mode: SinkMode,
    seen: Arc<Mutex<Vec<SeenWrite>>>,
}

impl RecordingSink {
    fn new(mode: SinkMode) -> Self {
        Self {
            mode,
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn seen(&self) -> Vec<SeenWrite> {
        self.seen.lock().expect("the sink's log").clone()
    }
}

impl McpArtifactSink for RecordingSink {
    fn put_result<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        request: AgentArtifactWriteRequest,
    ) -> McpArtifactFuture<'a> {
        Box::pin(async move {
            self.seen.lock().expect("the sink's log").push(SeenWrite {
                scope_key: scope.key(),
                request: request.clone(),
            });
            if self.mode == SinkMode::Failing {
                return Err(AgentArtifactError::Store {
                    message: "the artifact store is unavailable".to_string(),
                });
            }
            let digest = AgentContentDigest::sha256_of_bytes(&request.bytes).value;
            let uri = if self.mode == SinkMode::HugeUri {
                format!("host://artifacts/{}/{digest}", "p".repeat(3_000))
            } else {
                format!("host://artifacts/{digest}")
            };
            Ok(ArtifactRef {
                artifact_id: digest.clone(),
                kind: request.kind,
                uri,
                checksum: Some(format!("sha256:{digest}")),
                content_type: request.content_type,
                byte_len: Some(u64::try_from(request.bytes.len()).unwrap_or(u64::MAX)),
                retention_class: request.retention_class,
                encryption: None,
                redaction: if self.mode == SinkMode::UnknownRedaction {
                    RedactionStatus::Unknown
                } else {
                    request.redaction
                },
                created_at: request.created_at,
                metadata: AgentAttributes::default(),
            })
        })
    }
}

fn server_id() -> McpServerId {
    McpServerId::new("crm").expect("id")
}

fn policy(behavior: AgentToolResultBehavior) -> McpToolPolicy {
    McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly))
        .with_result_behavior(behavior)
}

fn call(tool: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new("call-1").expect("id"),
        AgentToolId::new(format!("mcp.crm.{tool}")).expect("id"),
        json!({}),
    )
    .expect("call")
}

/// The longest text a `{"text": …}` result may carry and still be inline:
/// derived from the run's own bound and the run's own measure, so the proof
/// holds whatever the wrapper costs.
fn longest_inline_text() -> usize {
    let empty = AgentTaskContent::inline(json!({"text": ""}))
        .expect("inline")
        .size_bytes();
    AGENT_TOOL_RESULT_MAX_BYTES - empty
}

fn text_tool(name: &str, text: String) -> FakeTool {
    FakeTool::new(
        name,
        "Answers with text.",
        json!({"type":"object"}),
        FakeToolBehaviour::Text(text),
    )
}

/// One tool per case. `big` and `leaky` are stored; the `edge-*` pair sits on
/// either side of the inline bound.
fn fake() -> FakeMcpServer {
    let edge = longest_inline_text();
    FakeMcpServer::new()
        .with_tool(text_tool("big", "x".repeat(5_000)))
        .with_tool(text_tool(
            "leaky",
            format!("{ECHOED_TOKEN} {}", "x".repeat(5_000)),
        ))
        .with_tool(text_tool("edge-in", "x".repeat(edge)))
        .with_tool(text_tool("edge-out", "x".repeat(edge + 1)))
        .with_tool(text_tool("edge-stored", "x".repeat(edge + 1)))
}

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(server_id(), url)
        .with_tool("big", policy(AgentToolResultBehavior::ArtifactReference))
        .expect("t")
        .with_tool("leaky", policy(AgentToolResultBehavior::ArtifactReference))
        .expect("t")
        .with_tool("edge-in", policy(AgentToolResultBehavior::InlineBounded))
        .expect("t")
        .with_tool("edge-out", policy(AgentToolResultBehavior::InlineBounded))
        .expect("t")
        .with_tool(
            "edge-stored",
            policy(AgentToolResultBehavior::ArtifactReference),
        )
        .expect("t")
}

async fn executor_over(
    url: &str,
    artifacts: impl Into<McpArtifacts>,
) -> McpDispatchToolExecutor<CountingClient> {
    let binding = binding(url);
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
    McpDispatchToolExecutor::new(
        vec![set],
        vec![binding],
        artifacts,
        http,
        Arc::new(McpAllowAllEgress),
    )
    .expect("builds")
}

#[tokio::test]
async fn a_sink_that_mints_its_own_id_is_what_the_attempt_answers() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::ContentAddressed);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink.clone())).await;
    let intent = tool_intent_with_timeout("mcp.crm.big", Some(5_000));
    let content = executor
        .execute(&run_scope(), &intent, &call("big"), None)
        .await
        .expect("artifact");
    let AgentTaskContent::Artifact(reference) = content else {
        panic!("expected an artifact, got {content:?}")
    };

    let seen = sink.seen();
    assert_eq!(seen.len(), 1, "one write");
    assert_eq!(
        seen[0].scope_key,
        run_scope().key(),
        "the sink is handed the run the attempt belongs to"
    );
    assert_eq!(seen[0].request.kind, ArtifactKind::ToolOutput);
    assert_eq!(
        seen[0].request.redaction,
        RedactionStatus::ReferenceOnly,
        "nothing was scrubbed, so nothing is marked redacted"
    );
    assert_eq!(
        seen[0].request.artifact_id,
        Some(format!(
            "mcp-{}-g{}-call-1",
            intent.effect_id,
            intent.generation.get()
        )),
        "the derived id is still requested"
    );
    assert_eq!(
        reference.artifact_id,
        AgentContentDigest::sha256_of_bytes(&seen[0].request.bytes).value,
        "and the sink's own id is what the attempt answers"
    );
}

#[tokio::test]
async fn a_reference_the_default_policy_refuses_fails_the_attempt() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::UnknownRedaction);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink)).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.big", Some(5_000)),
            &call("big"),
            None,
        )
        .await
        .expect_err("an unknown redaction status is not a recordable reference");
    assert!(
        error.to_string().contains("invalid-artifact-reference"),
        "{error}"
    );
}

#[tokio::test]
async fn a_sink_failure_fails_the_attempt_under_the_stores_own_code() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::Failing);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink)).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.big", Some(5_000)),
            &call("big"),
            None,
        )
        .await
        .expect_err("the store is down");
    assert!(error.to_string().contains("artifact-store"), "{error}");
}

#[tokio::test]
async fn a_scrubbed_result_is_stored_marked_redacted() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::ContentAddressed);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink.clone())).await;
    let credential = AgentEphemeralCredential::bearer_token(ECHOED_TOKEN);
    executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.leaky", Some(5_000)),
            &call("leaky"),
            Some(&credential),
        )
        .await
        .expect("artifact");
    let seen = sink.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].request.redaction, RedactionStatus::Redacted);
    let stored = String::from_utf8(seen[0].request.bytes.clone()).expect("json");
    assert!(!stored.contains(ECHOED_TOKEN), "the token was scrubbed");
    assert!(stored.contains("<redacted>"), "{stored}");
}

#[tokio::test]
async fn a_result_on_the_inline_edge_is_measured_as_the_run_measures_it() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::ContentAddressed);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink.clone())).await;

    let inline = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.edge-in", Some(5_000)),
            &call("edge-in"),
            None,
        )
        .await
        .expect("the longest inline result");
    assert!(inline.inline_value().is_some(), "{inline:?}");
    assert_eq!(
        inline.size_bytes(),
        AGENT_TOOL_RESULT_MAX_BYTES,
        "exactly the bound the run enforces"
    );
    assert_eq!(MCP_INLINE_RESULT_MAX_BYTES, AGENT_TOOL_RESULT_MAX_BYTES);

    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.edge-out", Some(5_000)),
            &call("edge-out"),
            None,
        )
        .await
        .expect_err("one byte over, and the binding keeps results inline");
    assert!(
        error.to_string().contains("mcp-result-too-large"),
        "{error}"
    );

    let stored = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.edge-stored", Some(5_000)),
            &call("edge-stored"),
            None,
        )
        .await
        .expect("one byte over, and the binding stores it");
    assert!(stored.artifact_ref().is_some(), "{stored:?}");
    assert!(stored.size_bytes() <= AGENT_TOOL_RESULT_MAX_BYTES);
    assert_eq!(sink.seen().len(), 1, "only the stored one was written");
}

#[tokio::test]
async fn a_reference_larger_than_the_runs_bound_is_refused() {
    let endpoint = serve_fake(fake()).await;
    let sink = RecordingSink::new(SinkMode::HugeUri);
    let executor = executor_over(&endpoint.url, McpArtifacts::sink(sink)).await;
    let error = executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.big", Some(5_000)),
            &call("big"),
            None,
        )
        .await
        .expect_err("the run would refuse this reference");
    assert!(
        error.to_string().contains("mcp-result-too-large"),
        "{error}"
    );
}

#[tokio::test]
async fn a_store_handle_still_builds_an_executor() {
    let endpoint = serve_fake(fake()).await;
    let store = SharedArtifactStore::default();
    let executor = executor_over(&endpoint.url, mcp_artifact_store(store.clone())).await;
    executor
        .execute(
            &run_scope(),
            &tool_intent_with_timeout("mcp.crm.big", Some(5_000)),
            &call("big"),
            None,
        )
        .await
        .expect("artifact");
    assert_eq!(
        store.len().await,
        1,
        "the store behind the handle was written"
    );
}
