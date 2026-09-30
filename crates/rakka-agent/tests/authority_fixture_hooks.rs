//! The two `AuthorityFixture` hooks a deployment-shaped test needs: register
//! tool bindings after the fixture was built, and put an executor of the
//! test's own in the dispatch pipeline's one executor slot.
//!
//! Both are fixture plumbing, so what is proven here is that the plumbing
//! reaches the production path: a binding registered through the hook is the
//! one the *dispatch gate's* authority sees, and the executor installed
//! through the hook is the one a real `start()`/`pump()` invokes.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, RecordingToolExecutor};
use rakka_agent::{
    AgentModelTurn, AgentTaskContent, AgentToolBinding, AgentToolExecutorRouter, AgentToolId,
    AgentToolKind, AgentToolRegistry, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;
use common::{tool_call, tool_descriptor_of_kind, AuthorityFixture};

/// The tool these proofs register and route: an MCP-kind binding under the
/// `mcp.` prefix every MCP binding carries.
const ROUTED_TOOL: &str = "mcp.crm.search";

fn routed_binding() -> AgentToolBinding {
    AgentToolBinding::unclassified(tool_descriptor_of_kind(
        ROUTED_TOOL,
        AgentToolKind::RemoteMcp,
    ))
}

fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Looking it up.")
        .with_tool_call(tool_call(ROUTED_TOOL))
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "found" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// The registration hook widens the authority the fixture hands the dispatch
/// gate, not only its own copy of the registry: a binding registered after the
/// fixture was built is dispatchable through the real pipeline.
#[test]
fn a_registered_binding_reaches_the_authority_behind_the_gate() {
    let tool = AgentToolId::new(ROUTED_TOOL).expect("tool id");
    let fixture = AuthorityFixture::over(
        DeterministicModelAdapter::new(),
        AgentToolRegistry::new(),
        None,
    )
    .with_registered_bindings(vec![routed_binding()]);

    assert!(
        fixture.authority.registry().binding(&tool).is_some(),
        "the gate's authority holds the registered binding"
    );
    assert!(
        fixture.envelope.tools.contains_key(&tool),
        "the envelope the agent is instantiated under declares it"
    );
}

/// The two hooks together, on the production path: the run commits the intent,
/// the outbox ticket is leased, the gate authorizes the registered binding —
/// and the router, sitting in the dispatcher's one executor slot, hands the
/// attempt to the executor its prefix route names rather than to the fixture's
/// default one.
#[tokio::test]
async fn the_pipeline_dispatches_a_routed_tool_through_the_installed_router() {
    let routed = RecordingToolExecutor::new();
    let fixture = AuthorityFixture::over(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn())
            .with_turn_for(2, proposing_turn()),
        AgentToolRegistry::new(),
        None,
    )
    .with_registered_bindings(vec![routed_binding()]);
    let router = AgentToolExecutorRouter::new(Arc::new(fixture.tools.clone()))
        .with_prefix_route("mcp.", Arc::new(routed.clone()));
    let fixture = fixture.with_tool_executor(Arc::new(router));

    fixture.start().await;
    fixture.pump().await;

    assert_eq!(
        routed.invocation_count(ROUTED_TOOL),
        1,
        "the prefix route's executor performed the call"
    );
    assert_eq!(
        fixture.tools.invocation_count(ROUTED_TOOL),
        0,
        "the fixture's default executor is the router's fallback, and was not reached"
    );
}
