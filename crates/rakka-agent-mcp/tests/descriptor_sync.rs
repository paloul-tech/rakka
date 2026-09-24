//! Publish-time descriptor sync: a standalone function over the injected
//! client, no store, no registry; the allow-list, the bounds, the hint rule,
//! the peer-agent rule, round-tripping, and the fact that a registry built
//! from the stored set makes no network call.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use rakka_agent::AgentAuthorityRefusal;
use rakka_agent::{
    AgentEffectSafetyClass, AgentToolDeclaration, AgentToolKind, AgentToolRegistry,
    AGENT_TOOL_PARAMETERS_MAX_BYTES,
};
use rakka_agent_mcp::testkit::{serve_fake, CountingClient, FakeMcpServer, FakeTool};
use rakka_agent_mcp::{
    mcp_descriptor_staleness, sync_mcp_descriptors, McpAllowAllEgress, McpDescriptorSet,
    McpDescriptorStaleness, McpEgressCheck, McpServerBinding, McpServerId, McpToolPolicy,
    MCP_ATTEMPT_TIMEOUT_DEFAULT_MS, MCP_DESCRIPTOR_SCHEMA_MAX_BYTES,
};
use rakka_agent_workflow::{AgentEphemeralCredential, AgentTimestampMillis};
use rmcp::model::ToolAnnotations;
use serde_json::json;

fn schema(props: usize) -> serde_json::Value {
    let mut properties = serde_json::Map::new();
    for i in 0..props {
        properties.insert(
            format!("p{i}"),
            json!({ "type": "string", "description": "x".repeat(40) }),
        );
    }
    json!({ "type": "object", "properties": properties })
}

fn tool(name: &str) -> FakeTool {
    FakeTool::text(name, "Does a thing.", schema(1), "ok")
}

fn declared(class: AgentEffectSafetyClass) -> McpToolPolicy {
    McpToolPolicy::new(AgentToolDeclaration::new(class))
}

fn client() -> rakka_agent_mcp::testkit::ReqwestClient {
    rakka_agent_mcp::testkit::ReqwestClient::new()
}

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), url)
        .with_tool("search", declared(AgentEffectSafetyClass::ReadOnly))
        .expect("tool")
        .with_tool("update", declared(AgentEffectSafetyClass::Idempotent))
        .expect("tool")
}

#[tokio::test]
async fn sync_returns_only_the_allow_listed_tools_as_prefixed_bindings_with_digests() {
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(tool("search"))
            .with_tool(tool("update"))
            .with_tool(tool("delete_everything")),
    )
    .await;
    let set = sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(7),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    let names: Vec<&str> = set.descriptors.iter().map(|d| d.tool.as_str()).collect();
    assert_eq!(
        names,
        vec!["search", "update"],
        "the unlisted tool is not registered"
    );
    let search = set.descriptor("search").expect("present");
    assert_eq!(search.binding.descriptor().tool.as_str(), "mcp.crm.search");
    assert_eq!(search.binding.descriptor().kind, AgentToolKind::RemoteMcp);
    assert_eq!(
        search.binding.declaration().safety,
        AgentEffectSafetyClass::ReadOnly
    );
    assert!(
        search.carries_inline_schema(),
        "a 1-property schema rides the descriptor"
    );
    assert_eq!(
        search.binding.descriptor().parameters.as_ref(),
        Some(&search.input_schema)
    );
    assert_eq!(
        search
            .binding
            .effect_spec()
            .expect("the synced binding's spec validates")
            .timeout_ms,
        Some(MCP_ATTEMPT_TIMEOUT_DEFAULT_MS),
        "the policy's default timeout rides the effect spec, so the dispatcher's deadline \
         and the executor's bound agree"
    );
    assert_eq!(set.synced_at, AgentTimestampMillis::new(7));
    assert_eq!(set.protocol_version, "2026-07-28");
    assert_eq!(endpoint.server.list_calls(), 1);
    let encoded = serde_json::to_string(&set).expect("encodes");
    let decoded: McpDescriptorSet = serde_json::from_str(&encoded).expect("decodes");
    assert_eq!(decoded, set);
    assert_eq!(decoded.digest(), set.digest());
}

#[tokio::test]
async fn a_registry_built_from_the_stored_set_makes_no_network_call() {
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(tool("search"))
            .with_tool(tool("update")),
    )
    .await;
    let set = sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    let before = endpoint.server.list_calls();
    let mut registry = AgentToolRegistry::new();
    for binding in set.bindings() {
        registry = registry.register(binding.clone()).expect("registers");
    }
    assert!(registry
        .binding(&rakka_agent::AgentToolId::new("mcp.crm.search").expect("id"))
        .is_some());
    assert_eq!(
        endpoint.server.list_calls(),
        before,
        "Manual refresh: no call at registry construction"
    );
}

#[tokio::test]
async fn the_credential_reaches_the_wire_only_as_the_declared_header() {
    // Both tools: `binding` allow-lists `search` and `update`, and a tool the
    // server does not list is its own refusal.
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(tool("search"))
            .with_tool(tool("update")),
    )
    .await;
    let credential = AgentEphemeralCredential::bearer_token("sync-token-sentinel");
    sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        Some(&credential),
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    let seen = endpoint.server.seen_headers();
    assert!(
        seen.iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("authorization")
                && value.ends_with("sync-token-sentinel")),
        "{seen:?}"
    );
    let basic = AgentEphemeralCredential::basic("u", "p");
    let error = sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        Some(&basic),
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect_err("basic is not a header");
    assert_eq!(error.code(), "mcp-credential-material-unsupported");
    assert!(
        !error.to_string().contains('p'),
        "no material in the message: {error}"
    );
}

#[tokio::test]
async fn the_schema_bounds_are_the_4kib_inline_and_64kib_refusal_lines() {
    let big_inline = schema(200); // > 4 KiB, < 64 KiB
    assert!(
        serde_json::to_vec(&big_inline).expect("bytes").len() > AGENT_TOOL_PARAMETERS_MAX_BYTES
    );
    let huge = schema(3000);
    assert!(serde_json::to_vec(&huge).expect("bytes").len() > MCP_DESCRIPTOR_SCHEMA_MAX_BYTES);
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(FakeTool::text("search", "d", big_inline.clone(), "ok"))
            .with_tool(FakeTool::text("update", "d", huge, "ok")),
    )
    .await;
    let only_search =
        McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), &endpoint.url)
            .with_tool("search", declared(AgentEffectSafetyClass::ReadOnly))
            .expect("tool");
    let set = sync_mcp_descriptors(
        &client(),
        &only_search,
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    let search = set.descriptor("search").expect("present");
    assert!(
        !search.carries_inline_schema(),
        "over 4 KiB the descriptor carries no parameters"
    );
    assert_eq!(
        search.input_schema, big_inline,
        "but the raw schema is returned for the caller to store"
    );
    let error = sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect_err("64 KiB");
    assert_eq!(error.code(), "mcp-descriptor-schema-too-large");
}

#[tokio::test]
async fn a_contradicting_hint_refuses_only_when_hints_are_honored() {
    let destructive = FakeTool::text("update", "d", schema(1), "ok")
        .with_annotations(ToolAnnotations::new().destructive(true));
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(tool("search"))
            .with_tool(destructive),
    )
    .await;
    let ignoring = binding(&endpoint.url);
    sync_mcp_descriptors(
        &client(),
        &ignoring,
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("hints ignored by default");
    let honoring =
        McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), &endpoint.url)
            .with_tool(
                "update",
                declared(AgentEffectSafetyClass::Idempotent).with_honor_hints(true),
            )
            .expect("tool");
    let error = sync_mcp_descriptors(
        &client(),
        &honoring,
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect_err("contradiction");
    assert_eq!(error.code(), "mcp-hint-contradicts-declaration");
    let honoring_non_idempotent =
        McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), &endpoint.url)
            .with_tool(
                "update",
                declared(AgentEffectSafetyClass::NonIdempotent).with_honor_hints(true),
            )
            .expect("tool");
    let set = sync_mcp_descriptors(
        &client(),
        &honoring_non_idempotent,
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("no contradiction");
    assert_eq!(
        set.descriptor("update")
            .expect("present")
            .binding
            .declaration()
            .safety,
        AgentEffectSafetyClass::NonIdempotent,
        "a hint never changes a declaration"
    );
}

#[tokio::test]
async fn a_server_that_identifies_as_a_rakka_agent_is_refused() {
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(tool("search"))
            .with_server_name("rakka-agent-support"),
    )
    .await;
    let error = sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect_err("peer channel");
    assert_eq!(error.code(), "mcp-peer-agent-channel-refused");
}

#[tokio::test]
async fn version_negotiation_falls_back_to_the_next_listed_version_or_refuses() {
    let older = serve_fake(
        FakeMcpServer::new()
            .with_tool(tool("search"))
            .with_tool(tool("update"))
            .with_supported_versions(vec![rmcp::model::ProtocolVersion::V_2025_11_25]),
    )
    .await;
    let set = sync_mcp_descriptors(
        &client(),
        &binding(&older.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("falls back");
    assert_eq!(set.protocol_version, "2025-11-25");
    let ancient = serve_fake(
        FakeMcpServer::new()
            .with_tool(tool("search"))
            .with_tool(tool("update"))
            .with_supported_versions(vec![rmcp::model::ProtocolVersion::V_2024_11_05]),
    )
    .await;
    let error = sync_mcp_descriptors(
        &client(),
        &binding(&ancient.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect_err("no common version");
    assert_eq!(error.code(), "mcp-protocol-unsupported");
}

#[tokio::test]
async fn a_dead_endpoint_is_a_sync_failure_with_no_body_in_the_message() {
    let closed = McpServerBinding::streamable_http(
        McpServerId::new("crm").expect("id"),
        "http://127.0.0.1:9/mcp",
    )
    .with_tool("search", declared(AgentEffectSafetyClass::ReadOnly))
    .expect("tool");
    let error = sync_mcp_descriptors(
        &client(),
        &closed,
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect_err("dead");
    assert_eq!(error.code(), "mcp-descriptor-sync-failed");
    assert!(error.to_string().len() < 512);
}

#[tokio::test]
async fn staleness_reports_added_removed_and_changed_tools() {
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(tool("search"))
            .with_tool(tool("update")),
    )
    .await;
    let stored = sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    assert_eq!(
        mcp_descriptor_staleness(&stored, &stored),
        McpDescriptorStaleness::Fresh
    );
    endpoint.server.swap_tool_schema("update", schema(2));
    let fresh = sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(2),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    assert_eq!(
        mcp_descriptor_staleness(&stored, &fresh),
        McpDescriptorStaleness::Stale {
            changed: vec!["update".to_string()]
        }
    );
}

/// An egress rule that refuses everything, checking what it was asked about.
struct RefuseEveryDestination;

impl McpEgressCheck for RefuseEveryDestination {
    fn check(&self, server: &McpServerId, url: &str) -> Result<(), AgentAuthorityRefusal> {
        assert!(
            url.starts_with("http://127.0.0.1:"),
            "the check is given the real endpoint URL: {url}"
        );
        Err(AgentAuthorityRefusal::of(
            "egress-denied",
            format!("{server} is not a reachable destination"),
        ))
    }
}

#[tokio::test]
async fn a_refusing_egress_rule_stops_the_sync_before_any_request_is_made() {
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(tool("search"))
            .with_tool(tool("update")),
    )
    .await;
    let counting = CountingClient::new();
    let credential = AgentEphemeralCredential::bearer_token("never-sent-sentinel");
    let error = sync_mcp_descriptors(
        &counting,
        &binding(&endpoint.url),
        Some(&credential),
        AgentTimestampMillis::new(1),
        &RefuseEveryDestination,
    )
    .await
    .expect_err("egress refuses");

    assert_eq!(
        error.code(),
        "egress-denied",
        "the host's own code, carried through unchanged"
    );
    assert!(
        !error.to_string().contains("never-sent-sentinel"),
        "{error}"
    );
    assert_eq!(
        counting.sends(),
        0,
        "no transport method ran, so no credential left the process"
    );
    assert_eq!(endpoint.server.list_calls(), 0, "the server saw nothing");
    assert_eq!(endpoint.server.seen_headers(), Vec::new());

    // The same binding through the allow-all opt-out does reach the server, so
    // the refusal above is the rule firing rather than the fixture being
    // unreachable.
    sync_mcp_descriptors(
        &counting,
        &binding(&endpoint.url),
        Some(&credential),
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("the allow-all check admits it");
    assert!(counting.sends() > 0);
    assert_eq!(endpoint.server.list_calls(), 1);
}
