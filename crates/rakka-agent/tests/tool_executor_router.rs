//! The executor router composes tool executors behind the dispatcher's one
//! `Arc<dyn AgentDispatchToolExecutor>`: an exact tool id wins over a
//! prefix, a prefix over a registry-declared kind, and everything else
//! reaches the fallback. Routing is by identity, never by arguments.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, RecordingToolExecutor};
use rakka_agent::{
    AgentCredentialBindingRef, AgentDispatchToolExecutor, AgentEffectSafetyClass,
    AgentEnvironmentConcurrencyProtocol, AgentGuardrailStageId, AgentModelTurn,
    AgentReconciliationProtocolRef, AgentTaskContent, AgentToolBinding, AgentToolCallId,
    AgentToolCallRequest, AgentToolDeclaration, AgentToolDescriptor, AgentToolExecutorRouter,
    AgentToolId, AgentToolKind, AgentToolRegistry, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;
use common::{run_scope, schema, tool_intent, AuthorityFixture};

/// The tool the pipeline proof routes: an MCP-kind binding under the `mcp.`
/// prefix every MCP binding carries.
const ROUTED_TOOL: &str = "mcp.crm.search";

fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Looking it up.")
        .with_tool_call(call(ROUTED_TOOL))
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "found" }))
                .expect("the proposal is inline-bounded"),
        )
}

fn descriptor(id: &str, kind: AgentToolKind) -> AgentToolDescriptor {
    AgentToolDescriptor::new(
        AgentToolId::new(id).expect("tool id"),
        kind,
        format!("{id} descriptor"),
        schema("in"),
        schema("out"),
    )
    .expect("descriptor")
}

fn call(id: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new("call-1").expect("call id"),
        AgentToolId::new(id).expect("tool id"),
        serde_json::json!({}),
    )
    .expect("call")
}

async fn execute_via(router: &AgentToolExecutorRouter, id: &str) {
    router
        .execute(&run_scope(), &tool_intent(id), &call(id), None)
        .await
        .expect("the routed executor answers");
}

#[tokio::test]
async fn exact_beats_prefix_beats_kind_beats_fallback() {
    let exact = RecordingToolExecutor::new();
    let prefixed = RecordingToolExecutor::new();
    let by_kind = RecordingToolExecutor::new();
    let fallback = RecordingToolExecutor::new();
    let registry = AgentToolRegistry::new()
        .register(AgentToolBinding::unclassified(descriptor(
            "proc.tool",
            AgentToolKind::Process,
        )))
        .expect("registered");

    let router = AgentToolExecutorRouter::new(Arc::new(fallback.clone()))
        .with_prefix_route("mcp.", Arc::new(prefixed.clone()))
        .with_tool_route(
            AgentToolId::new("mcp.crm.exact").expect("id"),
            Arc::new(exact.clone()),
        )
        .with_kind_route(AgentToolKind::Process, registry, Arc::new(by_kind.clone()));

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
            &call("mcp.crm.search"),
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
    let registry = AgentToolRegistry::new()
        .register(AgentToolBinding::unclassified(descriptor(
            "function.tool",
            AgentToolKind::Function,
        )))
        .expect("registered");
    let router = AgentToolExecutorRouter::new(Arc::new(fallback.clone())).with_kind_route(
        AgentToolKind::Process,
        registry,
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

/// A binding carrying every optional field, so a round trip proves each one
/// survives rather than being silently defaulted back to the fail-safe value.
fn populated_binding() -> AgentToolBinding {
    AgentToolBinding::new(
        descriptor("mcp.crm.charge", AgentToolKind::RemoteMcp),
        AgentToolDeclaration::new(AgentEffectSafetyClass::Reconcileable).with_credential_binding(
            AgentCredentialBindingRef::new("crm-token").expect("credential binding"),
        ),
        3,
    )
    .with_reconciliation_protocol(
        AgentReconciliationProtocolRef::new("payment-ledger").expect("protocol ref"),
    )
    .with_environment_concurrency(AgentEnvironmentConcurrencyProtocol::Reconciliation)
    .with_timeout_ms(5_000)
    .with_guardrail(AgentGuardrailStageId::new("pii-redaction").expect("stage id"))
    .with_checkpoint_required()
    .with_authorization_required()
}

#[test]
fn every_field_of_a_binding_survives_a_round_trip() {
    let binding = populated_binding();
    let decoded: AgentToolBinding =
        serde_json::from_str(&serde_json::to_string(&binding).expect("encodes")).expect("decodes");
    assert_eq!(decoded, binding);
    assert_eq!(
        decoded
            .effect_spec()
            .expect("the decoded binding projects a spec"),
        binding.effect_spec().expect("the spec projects"),
        "the attempt policy the decoded binding dispatches under is the one that was encoded"
    );
}

#[test]
fn a_binding_round_trips_through_serde_and_is_revalidated_on_decode() {
    let binding =
        AgentToolBinding::unclassified(descriptor("mcp.crm.search", AgentToolKind::RemoteMcp));
    let encoded = serde_json::to_string(&binding).expect("encodes");
    let decoded: AgentToolBinding = serde_json::from_str(&encoded).expect("decodes");
    assert_eq!(decoded, binding);
    let tampered = encoded.replace("\"max_attempts\":1", "\"max_attempts\":0");
    assert!(tampered != encoded, "the fixture must hit the field");
    assert!(
        serde_json::from_str::<AgentToolBinding>(&tampered).is_err(),
        "a decoded binding is validated"
    );
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
    .with_registered_bindings(vec![AgentToolBinding::unclassified(descriptor(
        ROUTED_TOOL,
        AgentToolKind::RemoteMcp,
    ))]);

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
    .with_registered_bindings(vec![AgentToolBinding::unclassified(descriptor(
        ROUTED_TOOL,
        AgentToolKind::RemoteMcp,
    ))]);
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
