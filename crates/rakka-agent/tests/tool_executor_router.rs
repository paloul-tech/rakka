//! The executor router composes tool executors behind the dispatcher's one
//! `Arc<dyn AgentDispatchToolExecutor>`: an exact tool id wins over a
//! prefix, a prefix over a registry-declared kind, and everything else
//! reaches the fallback. Routing is by identity, never by arguments.
//!
//! This file is about *which executor a call reaches*. The wire shape of the
//! bindings a kind route reads lives in `tool_binding_serde.rs`, and the
//! fixture wiring that puts a router in a real pipeline's executor slot lives
//! in `authority_fixture_hooks.rs`.

use std::sync::Arc;

use rakka_agent::testkit::RecordingToolExecutor;
use rakka_agent::{
    AgentDispatchToolExecutor, AgentToolBinding, AgentToolExecutorRouter, AgentToolId,
    AgentToolKind, AgentToolRegistry,
};

mod common;
use common::{run_scope, tool_call, tool_descriptor_of_kind, tool_intent};

async fn execute_via(router: &AgentToolExecutorRouter, id: &str) {
    router
        .execute(&run_scope(), &tool_intent(id), &tool_call(id), None)
        .await
        .expect("the routed executor answers");
}

/// A registry holding one tool under the given kind, so a kind route has
/// something to read.
fn registry_holding(tool: &str, kind: AgentToolKind) -> AgentToolRegistry {
    AgentToolRegistry::new()
        .register(AgentToolBinding::unclassified(tool_descriptor_of_kind(
            tool, kind,
        )))
        .expect("the binding registers")
}

#[tokio::test]
async fn exact_beats_prefix_beats_kind_beats_fallback() {
    let exact = RecordingToolExecutor::new();
    let prefixed = RecordingToolExecutor::new();
    let by_kind = RecordingToolExecutor::new();
    let fallback = RecordingToolExecutor::new();

    let router = AgentToolExecutorRouter::new(Arc::new(fallback.clone()))
        .with_prefix_route("mcp.", Arc::new(prefixed.clone()))
        .with_tool_route(
            AgentToolId::new("mcp.crm.exact").expect("tool id"),
            Arc::new(exact.clone()),
        )
        .with_kind_route(
            AgentToolKind::Process,
            registry_holding("proc.tool", AgentToolKind::Process),
            Arc::new(by_kind.clone()),
        );

    execute_via(&router, "mcp.crm.exact").await;
    execute_via(&router, "mcp.crm.other").await;
    execute_via(&router, "proc.tool").await;
    execute_via(&router, "charge-card").await;

    let names = |executor: &RecordingToolExecutor| -> Vec<String> {
        executor
            .invocations()
            .iter()
            .map(|invocation| invocation.tool.clone())
            .collect()
    };
    assert_eq!(names(&exact), vec!["mcp.crm.exact"]);
    assert_eq!(names(&prefixed), vec!["mcp.crm.other"]);
    assert_eq!(names(&by_kind), vec!["proc.tool"]);
    assert_eq!(names(&fallback), vec!["charge-card"]);
}

#[tokio::test]
async fn the_longest_prefix_wins_and_the_credential_passes_through() {
    let short = RecordingToolExecutor::new();
    let long = RecordingToolExecutor::new();
    let router = AgentToolExecutorRouter::new(Arc::new(RecordingToolExecutor::new()))
        .with_prefix_route("mcp.", Arc::new(short.clone()))
        .with_prefix_route("mcp.crm.", Arc::new(long.clone()));
    let credential = rakka_agent_workflow::AgentEphemeralCredential::bearer_token("t");
    router
        .execute(
            &run_scope(),
            &tool_intent("mcp.crm.search"),
            &tool_call("mcp.crm.search"),
            Some(&credential),
        )
        .await
        .expect("answers");
    assert!(short.invocations().is_empty());
    let seen = long.invocations();
    assert_eq!(seen.len(), 1);
    assert!(
        seen[0].with_credential,
        "the credential reaches the routed executor for the attempt"
    );
}

/// A kind route answers only for the kind its own registry declares: the
/// registry is the authority on what a tool *is*, so a tool it does not hold
/// — or holds under another kind — falls through to the fallback rather than
/// being claimed by identity alone.
#[tokio::test]
async fn a_kind_route_claims_only_what_its_registry_declares_under_that_kind() {
    let by_kind = RecordingToolExecutor::new();
    let fallback = RecordingToolExecutor::new();
    let router = AgentToolExecutorRouter::new(Arc::new(fallback.clone())).with_kind_route(
        AgentToolKind::Process,
        registry_holding("function.tool", AgentToolKind::Function),
        Arc::new(by_kind.clone()),
    );

    execute_via(&router, "function.tool").await;
    execute_via(&router, "unregistered.tool").await;

    assert!(
        by_kind.invocations().is_empty(),
        "neither the other-kind tool nor the unregistered one is this route's"
    );
    assert_eq!(fallback.invocations().len(), 2);
}

/// An empty prefix matches every tool id, so installing it would be a second
/// catch-all in front of the real one. The builder drops it instead, and the
/// fallback stays the only catch-all.
#[tokio::test]
async fn an_empty_prefix_route_is_ignored_rather_than_shadowing_the_fallback() {
    let empty = RecordingToolExecutor::new();
    let fallback = RecordingToolExecutor::new();
    let router = AgentToolExecutorRouter::new(Arc::new(fallback.clone()))
        .with_prefix_route("", Arc::new(empty.clone()));

    execute_via(&router, "charge-card").await;

    assert!(
        empty.invocations().is_empty(),
        "the empty prefix routes nothing"
    );
    assert_eq!(fallback.invocations().len(), 1);
}
