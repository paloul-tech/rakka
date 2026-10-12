//! The facade's `agent-mcp-testkit` feature reaches the MCP adapter's test
//! kit, so a deployment's proofs take it through `rakka` alone.
#![cfg(feature = "agent-mcp-testkit")]

use rakka::agent_mcp::testkit::{FakeMcpServer, FakeTool};

#[test]
fn the_mcp_test_kit_is_reachable_through_the_facade() {
    // The facade does not depend on `serde_json`; a null schema is enough to
    // name the tool.
    let server = FakeMcpServer::new().with_tool(FakeTool::text(
        "search",
        "Searches.",
        Default::default(),
        "ok",
    ));
    assert_eq!(server.handshakes(), 0);
    assert_eq!(server.list_calls(), 0);
}
