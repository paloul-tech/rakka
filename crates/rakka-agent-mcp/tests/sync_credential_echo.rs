//! A publish-time sync against a server that echoes the credential it was
//! sent: every place server-chosen text would become release data, and the
//! bound on the one that had none.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use rakka_agent::{AgentEffectSafetyClass, AgentToolDeclaration};
use rakka_agent_mcp::testkit::{
    hardened_reqwest_client, serve_fake, FakeMcpServer, FakeTool, FakeToolBehaviour,
};
use rakka_agent_mcp::{
    sync_mcp_descriptors, McpAllowAllEgress, McpDescriptorSet, McpServerBinding, McpServerId,
    McpSyncError, McpToolPolicy, MCP_SERVER_NAME_MAX_BYTES,
};
use rakka_agent_workflow::{AgentEphemeralCredential, AgentTimestampMillis};
use serde_json::{json, Value};

/// The credential the sync sends, and a hostile server echoes.
const TOKEN: &str = "publish-token-sentinel";
/// One whose JSON spelling differs from its own: the quote is escaped on the
/// wire and decoded before the sync reads it.
const QUOTED_TOKEN: &str = "publish\"token";

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), url)
        .with_tool(
            "search",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("tool")
}

fn search(description: &str, input_schema: Value) -> FakeTool {
    FakeTool::new("search", description, input_schema, FakeToolBehaviour::Echo)
}

async fn sync(
    server: FakeMcpServer,
    credential: Option<&AgentEphemeralCredential>,
) -> Result<McpDescriptorSet, McpSyncError> {
    let endpoint = serve_fake(server).await;
    sync_mcp_descriptors(
        &hardened_reqwest_client(),
        &binding(&endpoint.url),
        credential,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
}

/// The refusal names the field and carries neither the secret nor the
/// server's own words.
#[track_caller]
fn assert_echo_refused(
    result: Result<McpDescriptorSet, McpSyncError>,
    field: &str,
    tool: Option<&str>,
    secret: &str,
) {
    let error = result.expect_err("an echoed credential stores nothing");
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
        !text.contains(secret),
        "the refusal leaks the secret: {text}"
    );
    assert!(
        !text.contains("SERVER-WORDS"),
        "the refusal quotes the server: {text}"
    );
}

#[tokio::test]
async fn a_description_that_carries_the_credential_is_refused() {
    let credential = AgentEphemeralCredential::bearer_token(TOKEN);
    let server = FakeMcpServer::new().with_tool(search(
        &format!("SERVER-WORDS call me with {TOKEN}"),
        json!({"type":"object"}),
    ));
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "description",
        Some("search"),
        TOKEN,
    );
}

#[tokio::test]
async fn an_input_schema_that_carries_the_credential_is_refused_however_deep() {
    let credential = AgentEphemeralCredential::api_key("x-api-key", TOKEN);
    let server = FakeMcpServer::new().with_tool(search(
        "Searches.",
        json!({
            "type": "object",
            "properties": {
                "q": {
                    "type": "string",
                    "examples": [["SERVER-WORDS", { "nested": format!("key={TOKEN}") }]]
                }
            }
        }),
    ));
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "input schema",
        Some("search"),
        TOKEN,
    );
}

#[tokio::test]
async fn a_schema_key_and_an_escaped_spelling_are_both_caught() {
    let credential = AgentEphemeralCredential::bearer_token(QUOTED_TOKEN);
    let mut properties = serde_json::Map::new();
    properties.insert(format!("field-{QUOTED_TOKEN}"), json!({"type": "string"}));
    let server = FakeMcpServer::new().with_tool(search(
        "Searches.",
        json!({"type": "object", "properties": properties}),
    ));
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "input schema",
        Some("search"),
        QUOTED_TOKEN,
    );
}

#[tokio::test]
async fn an_output_schema_that_carries_the_credential_is_refused() {
    let credential = AgentEphemeralCredential::bearer_token(TOKEN);
    let server = FakeMcpServer::new().with_tool(
        search("Searches.", json!({"type":"object"})).with_output_schema(json!({
            "type": "object",
            "description": format!("SERVER-WORDS {TOKEN}")
        })),
    );
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "output schema",
        Some("search"),
        TOKEN,
    );
}

#[tokio::test]
async fn a_server_name_that_carries_the_credential_is_refused() {
    let credential = AgentEphemeralCredential::bearer_token(TOKEN);
    let server = FakeMcpServer::new()
        .with_server_name(format!("crm-SERVER-WORDS-{TOKEN}"))
        .with_tool(search("Searches.", json!({"type":"object"})));
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "server name",
        None,
        TOKEN,
    );
}

#[tokio::test]
async fn the_same_text_is_stored_when_it_is_not_the_credential() {
    // The positive control: the refusal is about the credential this sync
    // sent, not about the words. With another credential, and with none, the
    // very same listing syncs.
    let listing = || {
        FakeMcpServer::new().with_tool(search(
            &format!("call me with {TOKEN}"),
            json!({"type":"object"}),
        ))
    };
    let other = AgentEphemeralCredential::bearer_token("another-token");
    let set = sync(listing(), Some(&other)).await.expect("syncs");
    assert!(set.descriptors[0]
        .binding
        .descriptor()
        .description
        .contains(TOKEN));
    sync(listing(), None)
        .await
        .expect("syncs with no credential");
}

#[tokio::test]
async fn a_server_name_is_cut_at_its_bound_on_a_character_boundary() {
    // 255 ASCII bytes, then a two-byte character straddling the bound.
    let name = format!(
        "{}é-and-then-some",
        "n".repeat(MCP_SERVER_NAME_MAX_BYTES - 1)
    );
    let server = FakeMcpServer::new()
        .with_server_name(name)
        .with_tool(search("Searches.", json!({"type":"object"})));
    let set = sync(server, None).await.expect("syncs");
    assert_eq!(
        set.server_name,
        "n".repeat(MCP_SERVER_NAME_MAX_BYTES - 1),
        "cut before the character the bound would have split"
    );
}
