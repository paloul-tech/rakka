//! The test fake's own knobs, as a deployment's proofs use them: an output
//! schema swapped after the fake served, a tool the server lists with no
//! description at all, and a count of the handshakes it completed.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use rakka_agent::{AgentEffectSafetyClass, AgentToolDeclaration};
use rakka_agent_mcp::testkit::{hardened_reqwest_client, serve_fake, FakeMcpServer, FakeTool};
use rakka_agent_mcp::{
    mcp_descriptor_staleness, sync_mcp_descriptors, McpAllowAllEgress, McpDescriptorSet,
    McpDescriptorStaleness, McpServerBinding, McpServerId, McpToolAnnotations, McpToolPolicy,
};
use rakka_agent_workflow::AgentTimestampMillis;
use serde_json::{json, Value};

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), url)
        .with_tool(
            "search",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("tool")
}

fn search() -> FakeTool {
    FakeTool::text("search", "Searches.", json!({ "type": "object" }), "ok")
}

async fn sync(url: &str) -> McpDescriptorSet {
    sync_mcp_descriptors(
        &hardened_reqwest_client(),
        &binding(url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs")
}

fn shape(name: &str) -> Value {
    json!({ "type": "object", "properties": { name: { "type": "string" } } })
}

#[tokio::test]
async fn an_output_schema_swapped_after_the_fake_served_reads_as_stale() {
    let endpoint =
        serve_fake(FakeMcpServer::new().with_tool(search().with_output_schema(shape("before"))))
            .await;
    let stored = sync(&endpoint.url).await;

    endpoint
        .server
        .swap_tool_output_schema("search", Some(shape("after")));
    let reshaped = sync(&endpoint.url).await;
    assert_eq!(reshaped.descriptors[0].output_schema, Some(shape("after")));
    assert_eq!(
        reshaped.descriptors[0].input_schema, stored.descriptors[0].input_schema,
        "only the output schema moved"
    );
    assert_eq!(
        mcp_descriptor_staleness(&stored, &reshaped),
        McpDescriptorStaleness::Stale {
            changed: vec!["search".to_string()]
        }
    );

    endpoint.server.swap_tool_output_schema("search", None);
    let dropped = sync(&endpoint.url).await;
    assert_eq!(dropped.descriptors[0].output_schema, None);
    assert_eq!(dropped.descriptors[0].output_schema_digest, None);
}

#[test]
#[should_panic(expected = "the fake server declares no tool named missing")]
fn swapping_the_output_schema_of_an_undeclared_tool_panics() {
    FakeMcpServer::new()
        .with_tool(search())
        .swap_tool_output_schema("missing", None);
}

#[tokio::test]
async fn a_tool_listed_without_a_description_syncs_as_not_supplied() {
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(search().without_description())).await;
    let set = sync(&endpoint.url).await;
    assert_eq!(set.descriptors[0].description_supplied, Some(false));
    assert_eq!(
        set.descriptors[0].binding.descriptor().description,
        "MCP tool search on crm",
        "the adapter's fallback stands in for the description the server never sent"
    );
}

#[tokio::test]
async fn each_completed_handshake_is_counted_once() {
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(search())).await;
    assert_eq!(endpoint.server.handshakes(), 0);
    sync(&endpoint.url).await;
    assert_eq!(endpoint.server.handshakes(), 1, "one sync, one handshake");
    sync(&endpoint.url).await;
    assert_eq!(endpoint.server.handshakes(), 2);
}

#[tokio::test]
async fn a_legacy_handshake_counts_once_though_discovery_was_refused_first() {
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(search()).with_legacy_only()).await;
    sync(&endpoint.url).await;
    assert_eq!(
        endpoint.server.handshakes(),
        1,
        "the refused server/discover probe is not a handshake; the initialize is"
    );
}

#[tokio::test]
async fn annotations_set_without_rmcp_are_listed_and_stored() {
    let annotations = McpToolAnnotations {
        title: Some("Search".to_string()),
        read_only_hint: Some(true),
        destructive_hint: Some(false),
        idempotent_hint: Some(true),
        open_world_hint: Some(false),
    };
    let endpoint =
        serve_fake(FakeMcpServer::new().with_tool(search().with_annotations(annotations.clone())))
            .await;
    let set = sync(&endpoint.url).await;
    assert_eq!(set.descriptors[0].annotations, Some(annotations));
}
