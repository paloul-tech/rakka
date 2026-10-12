//! `list_mcp_tools`: every tool a server lists, vetted but not bound — under
//! the sync's own rules, with no allow-list, storing nothing.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use std::time::{Duration, Instant};

use rakka_agent::{
    AgentAuthorityRefusal, AgentContentDigest, AgentEffectSafetyClass, AgentToolDeclaration,
};
use rakka_agent_mcp::testkit::{
    hardened_reqwest_client, serve_fake, CountingClient, FakeMcpServer, FakeTool,
};
use rakka_agent_mcp::{
    list_mcp_tools, list_mcp_tools_within, McpAllowAllEgress, McpEgressCheck, McpListedToolRefusal,
    McpServerBinding, McpServerId, McpToolListing, McpToolPolicy, MCP_DESCRIPTOR_SCHEMA_MAX_BYTES,
    MCP_SESSION_CLOSE_BOUND_MS,
};
use rakka_agent_workflow::AgentEphemeralCredential;
use serde_json::{json, Value};

mod support;

fn server_id() -> McpServerId {
    McpServerId::new("crm").expect("id")
}

fn tool_less(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(server_id(), url)
}

fn object() -> Value {
    json!({ "type": "object" })
}

/// A schema whose encoding is over the 64 KiB line.
fn oversized() -> Value {
    json!({ "type": "object", "description": "x".repeat(MCP_DESCRIPTOR_SCHEMA_MAX_BYTES) })
}

async fn listed(server: FakeMcpServer) -> McpToolListing {
    let endpoint = serve_fake(server).await;
    list_mcp_tools(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        None,
        &McpAllowAllEgress,
    )
    .await
    .expect("lists")
}

#[tokio::test]
async fn a_listing_returns_every_tool_in_name_order_marking_the_ones_a_sync_would_refuse() {
    let long_name = "n".repeat(300);
    let listing = listed(
        FakeMcpServer::new()
            .with_server_version("4.2.0")
            .with_tool(
                FakeTool::text("search", "Searches.", object(), "ok").with_output_schema(object()),
            )
            .with_tool(FakeTool::text("big_input", "Big.", oversized(), "ok"))
            .with_tool(
                FakeTool::text("big_output", "Big out.", object(), "ok")
                    .with_output_schema(oversized()),
            )
            .with_tool(FakeTool::text(long_name.clone(), "Long.", object(), "ok"))
            .with_tool(FakeTool::text("bell\u{7}", "Rings.", object(), "ok"))
            .with_tool(FakeTool::text("quiet", "x", object(), "ok").without_description()),
    )
    .await;

    assert_eq!(listing.server_id, server_id());
    assert_eq!(listing.server_name, "fake-mcp-server");
    assert_eq!(listing.server_version.as_deref(), Some("4.2.0"));
    assert_eq!(listing.protocol_version, "2026-07-28");
    let names: Vec<&str> = listing
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    let cut = "n".repeat(256);
    assert_eq!(
        names,
        [
            "bell\u{7}",
            "big_input",
            "big_output",
            cut.as_str(),
            "quiet",
            "search"
        ]
    );
    let tool = |name: &str| {
        listing
            .tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("{name} is listed"))
    };

    let search = tool("search");
    assert_eq!(
        search.tool_id.as_ref().map(|id| id.as_str()),
        Some("mcp.crm.search")
    );
    assert_eq!(search.description.as_deref(), Some("Searches."));
    assert_eq!(search.input_schema, Some(object()));
    assert_eq!(
        search.schema_digest,
        Some(AgentContentDigest::sha256_of_json(&object()))
    );
    assert_eq!(search.output_schema, Some(object()));
    assert_eq!(
        search.output_schema_digest,
        Some(AgentContentDigest::sha256_of_json(&object()))
    );
    assert_eq!(search.unusable, None);

    for name in ["big_input", "big_output"] {
        let big = tool(name);
        assert!(
            matches!(big.unusable, Some(McpListedToolRefusal::SchemaTooLarge { bytes }) if bytes > MCP_DESCRIPTOR_SCHEMA_MAX_BYTES),
            "{name}: {:?}",
            big.unusable
        );
        assert!(big.tool_id.is_some(), "{name}'s name is bindable");
        assert!(big.description.is_some(), "{name} keeps its description");
        assert_eq!(
            (
                &big.input_schema,
                &big.schema_digest,
                &big.output_schema,
                &big.output_schema_digest
            ),
            (&None, &None, &None, &None),
            "{name}'s schemas are withheld"
        );
    }
    assert_eq!(
        tool("big_input")
            .unusable
            .as_ref()
            .map(McpListedToolRefusal::code),
        Some("mcp-descriptor-schema-too-large")
    );

    for name in [cut.as_str(), "bell\u{7}"] {
        let misnamed = tool(name);
        assert!(
            matches!(
                misnamed.unusable,
                Some(McpListedToolRefusal::NameInvalid { .. })
            ),
            "{:?}",
            misnamed.unusable
        );
        assert_eq!(misnamed.tool_id, None);
        assert_eq!(misnamed.input_schema, None);
        assert_eq!(
            misnamed.unusable.as_ref().map(McpListedToolRefusal::code),
            Some("mcp-binding-invalid")
        );
    }

    let quiet = tool("quiet");
    assert_eq!(quiet.description, None);
    assert_eq!(quiet.unusable, None);
}

#[tokio::test]
async fn a_name_at_the_tool_id_boundary_is_bindable_and_one_byte_more_is_not() {
    // `mcp.crm.` is 8 bytes; a tool id is at most 256.
    let fits = "a".repeat(248);
    let over = "b".repeat(249);
    let listing = listed(
        FakeMcpServer::new()
            .with_tool(FakeTool::text(fits.clone(), "Fits.", object(), "ok"))
            .with_tool(FakeTool::text(over.clone(), "Over.", object(), "ok")),
    )
    .await;
    assert_eq!(listing.tools[0].name, fits);
    assert!(listing.tools[0].tool_id.is_some());
    assert_eq!(listing.tools[0].unusable, None);
    assert_eq!(
        listing.tools[1].name, over,
        "under 256 bytes, returned whole"
    );
    assert!(matches!(
        listing.tools[1].unusable,
        Some(McpListedToolRefusal::NameInvalid { .. })
    ));
}

#[tokio::test]
async fn the_allow_list_is_not_read_so_a_named_tool_the_server_lacks_is_no_refusal() {
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(FakeTool::text(
        "search",
        "Searches.",
        object(),
        "ok",
    )))
    .await;
    let named = tool_less(&endpoint.url)
        .with_tool(
            "archive",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("tool");
    let listing = list_mcp_tools(&hardened_reqwest_client(), &named, None, &McpAllowAllEgress)
        .await
        .expect("lists regardless of the allow-list");
    let names: Vec<&str> = listing
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    assert_eq!(names, ["search"]);
}

#[tokio::test]
async fn a_server_with_no_tools_lists_as_empty() {
    let endpoint = serve_fake(FakeMcpServer::new()).await;
    let listing = list_mcp_tools(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        None,
        &McpAllowAllEgress,
    )
    .await
    .expect("an empty server is a listing, not a refusal");
    assert!(listing.tools.is_empty());
    assert_eq!(endpoint.server.handshakes(), 1);
    assert_eq!(endpoint.server.list_calls(), 1);
}

#[tokio::test]
async fn a_name_the_server_lists_twice_is_one_entry_from_its_first_copy() {
    let listing = listed(
        FakeMcpServer::new()
            .with_tool(FakeTool::text("search", "First.", object(), "ok"))
            .with_tool(FakeTool::text("search", "Second.", object(), "ok")),
    )
    .await;
    assert_eq!(listing.tools.len(), 1);
    assert_eq!(listing.tools[0].description.as_deref(), Some("First."));
}

#[tokio::test]
async fn an_empty_server_version_lists_as_none() {
    let listing = listed(FakeMcpServer::new().with_server_version("")).await;
    assert_eq!(listing.server_version, None);
}

struct RefuseEveryDestination;

impl McpEgressCheck for RefuseEveryDestination {
    fn check(&self, _server: &McpServerId, _url: &str) -> Result<(), AgentAuthorityRefusal> {
        Err(AgentAuthorityRefusal::of(
            "egress-denied",
            "not on the allowlist",
        ))
    }
}

#[tokio::test]
async fn a_refusing_egress_rule_stops_the_listing_before_any_request() {
    let endpoint = serve_fake(FakeMcpServer::new()).await;
    let counting = CountingClient::new();
    let credential = AgentEphemeralCredential::bearer_token("never-sent-sentinel");
    let error = list_mcp_tools(
        &counting,
        &tool_less(&endpoint.url),
        Some(&credential),
        &RefuseEveryDestination,
    )
    .await
    .expect_err("egress refuses");
    assert_eq!(error.code(), "egress-denied");
    assert_eq!(counting.sends(), 0, "no transport method ran");
    assert_eq!(endpoint.server.handshakes(), 0, "the server saw nothing");
}

#[tokio::test]
async fn the_credential_reaches_the_wire_as_its_header() {
    let endpoint = serve_fake(FakeMcpServer::new()).await;
    let credential = AgentEphemeralCredential::bearer_token("listing-token-sentinel");
    list_mcp_tools(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        Some(&credential),
        &McpAllowAllEgress,
    )
    .await
    .expect("lists");
    let seen = endpoint.server.seen_headers();
    assert!(
        seen.iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("authorization")
                && value.ends_with("listing-token-sentinel")),
        "{seen:?}"
    );
}

#[tokio::test]
async fn a_server_that_identifies_as_a_rakka_agent_is_refused() {
    let endpoint = serve_fake(FakeMcpServer::new().with_server_name("rakka-agent-support")).await;
    let error = list_mcp_tools(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        None,
        &McpAllowAllEgress,
    )
    .await
    .expect_err("peer channel");
    assert_eq!(error.code(), "mcp-peer-agent-channel-refused");
}

#[tokio::test]
async fn a_listing_that_stalls_ends_at_its_bound() {
    let endpoint = serve_fake(FakeMcpServer::new().with_list_delay(5_000)).await;
    let started = Instant::now();
    let error = list_mcp_tools_within(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        None,
        &McpAllowAllEgress,
        Duration::from_millis(300),
    )
    .await
    .expect_err("the listing never answered inside the bound");
    assert_eq!(error.code(), "mcp-descriptor-sync-failed", "{error}");
    assert!(
        started.elapsed() < Duration::from_millis(300 + MCP_SESSION_CLOSE_BOUND_MS + 1_700),
        "the bound, plus the session's own bounded close: {:?}",
        started.elapsed()
    );
    assert_eq!(endpoint.server.list_calls(), 1);
}

#[tokio::test]
async fn a_listing_whose_pages_never_end_is_refused() {
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(FakeTool::text("search", "Searches.", object(), "ok"))
            .with_endless_pages(),
    )
    .await;
    let error = list_mcp_tools(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        None,
        &McpAllowAllEgress,
    )
    .await
    .expect_err("endless pages");
    assert_eq!(error.code(), "mcp-descriptor-sync-failed", "{error}");
}

#[tokio::test]
async fn a_child_process_binding_is_refused_as_no_http_endpoint() {
    let child = McpServerBinding::child_process(server_id(), support::spec_artifact_ref());
    let counting = CountingClient::new();
    let error = list_mcp_tools(&counting, &child, None, &McpAllowAllEgress)
        .await
        .expect_err("the HTTP listing has no endpoint to dial");
    assert_eq!(error.code(), "mcp-descriptor-sync-failed", "{error}");
    assert_eq!(counting.sends(), 0);
}

#[tokio::test]
async fn a_binding_with_an_invalid_endpoint_is_refused_before_egress() {
    let counting = CountingClient::new();
    let error = list_mcp_tools(
        &counting,
        &tool_less("ftp://h/mcp"),
        None,
        &RefuseEveryDestination,
    )
    .await
    .expect_err("the URL rule");
    assert_eq!(
        error.code(),
        "mcp-binding-invalid",
        "the binding, not the egress rule: {error}"
    );
    assert_eq!(counting.sends(), 0);
}
