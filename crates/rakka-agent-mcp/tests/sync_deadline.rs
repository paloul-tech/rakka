//! A publish-time sync is bounded in time: a handshake that is never
//! answered, a listing that stalls, the default, and the session closed on
//! the way out.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use std::time::{Duration, Instant};

use rakka_agent::{AgentEffectSafetyClass, AgentToolDeclaration};
use rakka_agent_mcp::testkit::{
    hardened_reqwest_client, serve_fake, FakeMcpServer, FakeTool, FakeToolBehaviour,
};
use rakka_agent_mcp::{
    sync_mcp_descriptors, sync_mcp_descriptors_over_within, sync_mcp_descriptors_within,
    McpAllowAllEgress, McpServerBinding, McpServerId, McpToolPolicy, MCP_SYNC_TIMEOUT_DEFAULT_MS,
};
use rakka_agent_workflow::AgentTimestampMillis;
use serde_json::json;

mod support;
use support::duplex_transport;

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), url)
        .with_tool(
            "search",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("tool")
}

fn listing() -> FakeMcpServer {
    FakeMcpServer::new().with_tool(FakeTool::new(
        "search",
        "Searches.",
        json!({"type":"object"}),
        FakeToolBehaviour::Echo,
    ))
}

/// A socket that accepts every connection and never writes a byte.
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

#[track_caller]
fn assert_timed_out(error: &rakka_agent_mcp::McpSyncError, bound_ms: u64) {
    assert_eq!(error.code(), "mcp-descriptor-sync-failed", "{error}");
    assert!(
        error
            .to_string()
            .contains(&format!("the sync exceeded its {bound_ms} ms bound")),
        "{error}"
    );
}

#[tokio::test]
async fn a_handshake_that_is_never_answered_ends_at_the_bound() {
    let (url, silent) = silent_endpoint().await;
    let started = Instant::now();
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        sync_mcp_descriptors_within(
            &hardened_reqwest_client(),
            &binding(&url),
            None,
            AgentTimestampMillis::new(1),
            &McpAllowAllEgress,
            Duration::from_millis(200),
        ),
    )
    .await
    .expect("the sync returned on its own rather than waiting forever")
    .expect_err("the handshake was never answered");
    silent.abort();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the bound fired inside the handshake: {:?}",
        started.elapsed()
    );
    assert_timed_out(&error, 200);
}

#[tokio::test]
async fn a_listing_that_stalls_ends_at_the_bound_and_the_session_is_closed() {
    let endpoint = serve_fake(listing().with_list_delay(5_000)).await;
    let started = Instant::now();
    let error = sync_mcp_descriptors_within(
        &hardened_reqwest_client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
        Duration::from_millis(300),
    )
    .await
    .expect_err("the listing never answered inside the bound");
    // The bound, plus the session's own bounded close.
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_timed_out(&error, 300);
    assert_eq!(
        endpoint.server.list_calls(),
        1,
        "the listing was reached, and asked for once"
    );
}

#[tokio::test]
async fn a_sync_inside_its_bound_is_unchanged() {
    let endpoint = serve_fake(listing()).await;
    let bounded = sync_mcp_descriptors_within(
        &hardened_reqwest_client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
        Duration::from_secs(5),
    )
    .await
    .expect("syncs");
    let defaulted = sync_mcp_descriptors(
        &hardened_reqwest_client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    assert_eq!(bounded, defaulted);
    assert_eq!(MCP_SYNC_TIMEOUT_DEFAULT_MS, 30_000);
}

#[tokio::test]
async fn a_launcher_transport_sync_is_bounded_too() {
    let server = listing().with_list_delay(5_000);
    let child = McpServerBinding::child_process(
        McpServerId::new("crm").expect("id"),
        support::spec_artifact_ref(),
    )
    .with_tool(
        "search",
        McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
    )
    .expect("tool");
    let error = sync_mcp_descriptors_over_within(
        duplex_transport(server),
        &child,
        AgentTimestampMillis::new(1),
        Duration::from_millis(300),
    )
    .await
    .expect_err("the listing never answered inside the bound");
    assert_timed_out(&error, 300);
}
