//! A tool listing against a server that echoes the credential it was sent:
//! every piece of server text the listing received is judged, and any one
//! that carries it refuses the whole listing.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use rakka_agent_mcp::testkit::{hardened_reqwest_client, serve_fake, FakeMcpServer, FakeTool};
use rakka_agent_mcp::{
    list_mcp_tools, McpAllowAllEgress, McpServerBinding, McpServerId, McpSyncError, McpToolListing,
    MCP_DESCRIPTOR_SCHEMA_MAX_BYTES,
};
use rakka_agent_workflow::AgentEphemeralCredential;
use rmcp::model::ToolAnnotations;
use serde_json::{json, Value};

/// The credential the listing sends, and a hostile server echoes.
const TOKEN: &str = "listing-token-sentinel";

fn object() -> Value {
    json!({ "type": "object" })
}

fn search() -> FakeTool {
    FakeTool::text("search", "Searches.", object(), "ok")
}

async fn list(
    server: FakeMcpServer,
    credential: Option<&AgentEphemeralCredential>,
) -> Result<McpToolListing, McpSyncError> {
    let endpoint = serve_fake(server).await;
    list_mcp_tools(
        &hardened_reqwest_client(),
        &McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), &endpoint.url),
        credential,
        &McpAllowAllEgress,
    )
    .await
}

/// The refusal names the field and carries neither the secret nor the
/// server's own words.
#[track_caller]
fn assert_echo_refused(
    result: Result<McpToolListing, McpSyncError>,
    field: &str,
    tool: Option<&str>,
) {
    let error = result.expect_err("an echoed credential refuses the listing");
    assert_eq!(error.code(), "mcp-descriptor-credential-echoed", "{error}");
    let McpSyncError::CredentialEchoed {
        field: refused,
        tool: named,
        ..
    } = &error
    else {
        panic!("expected CredentialEchoed, got {error:?}")
    };
    assert_eq!(*refused, field);
    assert_eq!(named.as_deref(), tool);
    let text = format!("{error} {error:?}");
    assert!(
        !text.contains(TOKEN),
        "the refusal leaks the secret: {text}"
    );
    assert!(
        !text.contains("SERVER-WORDS"),
        "the refusal quotes the server: {text}"
    );
}

fn bearer() -> AgentEphemeralCredential {
    AgentEphemeralCredential::bearer_token(TOKEN)
}

#[tokio::test]
async fn a_server_name_that_carries_the_credential_is_refused() {
    let server = FakeMcpServer::new()
        .with_server_name(format!("crm-SERVER-WORDS-{TOKEN}"))
        .with_tool(search());
    assert_echo_refused(list(server, Some(&bearer())).await, "server name", None);
}

#[tokio::test]
async fn a_server_version_that_carries_the_credential_is_refused() {
    let server = FakeMcpServer::new()
        .with_server_version(format!("1.0-SERVER-WORDS-{TOKEN}"))
        .with_tool(search());
    assert_echo_refused(list(server, Some(&bearer())).await, "server version", None);
}

#[tokio::test]
async fn a_tool_name_that_carries_the_credential_is_refused_without_naming_it() {
    let server = FakeMcpServer::new().with_tool(FakeTool::text(
        format!("SERVER-WORDS-{TOKEN}"),
        "Leaks.",
        object(),
        "ok",
    ));
    assert_echo_refused(list(server, Some(&bearer())).await, "tool name", None);
}

#[tokio::test]
async fn a_description_that_carries_the_credential_is_refused() {
    let server = FakeMcpServer::new().with_tool(FakeTool::text(
        "search",
        format!("SERVER-WORDS call me with {TOKEN}"),
        object(),
        "ok",
    ));
    assert_echo_refused(
        list(server, Some(&bearer())).await,
        "description",
        Some("search"),
    );
}

#[tokio::test]
async fn an_annotation_title_that_carries_the_credential_is_refused() {
    let server = FakeMcpServer::new().with_tool(
        search().with_annotations(ToolAnnotations::with_title(format!("SERVER-WORDS {TOKEN}"))),
    );
    assert_echo_refused(
        list(server, Some(&bearer())).await,
        "annotations title",
        Some("search"),
    );
}

#[tokio::test]
async fn an_input_schema_that_carries_the_credential_is_refused_however_deep() {
    let server = FakeMcpServer::new().with_tool(FakeTool::text(
        "search",
        "Searches.",
        json!({
            "type": "object",
            "properties": { "q": { "examples": [["SERVER-WORDS", { "nested": format!("key={TOKEN}") }]] } }
        }),
        "ok",
    ));
    assert_echo_refused(
        list(server, Some(&bearer())).await,
        "input schema",
        Some("search"),
    );
}

#[tokio::test]
async fn an_output_schema_that_carries_the_credential_is_refused() {
    let server = FakeMcpServer::new().with_tool(search().with_output_schema(json!({
        "type": "object",
        "description": format!("SERVER-WORDS {TOKEN}")
    })));
    assert_echo_refused(
        list(server, Some(&bearer())).await,
        "output schema",
        Some("search"),
    );
}

#[tokio::test]
async fn a_schema_the_listing_would_withhold_is_still_judged() {
    let server = FakeMcpServer::new().with_tool(FakeTool::text(
        "big",
        "Big.",
        json!({
            "type": "object",
            "description": format!("SERVER-WORDS {TOKEN} {}", "x".repeat(MCP_DESCRIPTOR_SCHEMA_MAX_BYTES))
        }),
        "ok",
    ));
    assert_echo_refused(
        list(server, Some(&bearer())).await,
        "input schema",
        Some("big"),
    );
}

#[tokio::test]
async fn a_duplicates_second_copy_is_still_judged() {
    let server = FakeMcpServer::new()
        .with_tool(search())
        .with_tool(FakeTool::text(
            "search",
            format!("SERVER-WORDS {TOKEN}"),
            object(),
            "ok",
        ));
    assert_echo_refused(
        list(server, Some(&bearer())).await,
        "description",
        Some("search"),
    );
}

#[tokio::test]
async fn the_same_text_lists_when_it_is_not_the_credential() {
    // The positive control: the refusal is about the credential this listing
    // sent, not about the words. With another credential, and with none, the
    // very same listing answers.
    let server = || {
        FakeMcpServer::new()
            .with_server_version(format!("1.0-{TOKEN}"))
            .with_tool(FakeTool::text(
                format!("tool-{TOKEN}"),
                format!("call me with {TOKEN}"),
                object(),
                "ok",
            ))
    };
    let other = AgentEphemeralCredential::bearer_token("another-token");
    let listing = list(server(), Some(&other)).await.expect("lists");
    assert_eq!(listing.tools.len(), 1);
    list(server(), None)
        .await
        .expect("lists with no credential");
}
