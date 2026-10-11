//! A stored descriptor set checks itself (issue #85, ask 10 and its
//! addendum): on every decode, whatever its version, and at executor
//! construction. Each test starts from a set a healthy server's sync
//! produced and breaks exactly one thing.
#![cfg(feature = "testkit")]

use std::sync::Arc;

use rakka_agent::{
    AgentContentDigest, AgentEffectSafetyClass, AgentToolBinding, AgentToolDeclaration,
    AgentToolDescriptor, AgentToolId, AgentToolKind,
};
use rakka_agent_mcp::testkit::{hardened_reqwest_client, serve_fake, FakeMcpServer, FakeTool};
use rakka_agent_mcp::{
    mcp_artifact_store, mcp_descriptor_staleness, sync_mcp_descriptors, McpAllowAllEgress,
    McpDescriptorSet, McpDescriptorStaleness, McpDispatchToolExecutor, McpServerBinding,
    McpServerId, McpSyncedDescriptor, McpToolPolicy,
};
use rakka_agent_workflow::AgentTimestampMillis;
use serde_json::{json, Value};

mod support;
use support::SharedArtifactStore;

fn binding(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), url)
        .with_tool(
            "search",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("tool")
        .with_tool(
            "update",
            McpToolPolicy::new(AgentToolDeclaration::new(
                AgentEffectSafetyClass::Idempotent,
            )),
        )
        .expect("tool")
}

/// A set a healthy server's sync produced, and the binding it was synced
/// for. `search` declares an output schema; both tools ride inline.
async fn synced() -> (McpDescriptorSet, McpServerBinding) {
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(
                FakeTool::text(
                    "search",
                    "Searches.",
                    json!({ "type": "object", "properties": { "q": { "type": "string" } } }),
                    "ok",
                )
                .with_output_schema(json!({ "type": "object" })),
            )
            .with_tool(FakeTool::text(
                "update",
                "Updates.",
                json!({ "type": "object" }),
                "ok",
            )),
    )
    .await;
    let binding = binding(&endpoint.url);
    let set = sync_mcp_descriptors(
        &hardened_reqwest_client(),
        &binding,
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    (set, binding)
}

/// `original`'s binding with its descriptor edited by `edit`.
fn rebound(
    original: &McpSyncedDescriptor,
    edit: impl FnOnce(&mut AgentToolDescriptor),
) -> AgentToolBinding {
    let mut descriptor = original.binding.descriptor().clone();
    edit(&mut descriptor);
    AgentToolBinding::new(descriptor, original.binding.declaration().clone(), 1)
}

/// Encodes `set` (encoding never checks) and asserts the decode refuses it
/// under the code, naming `reason`, the server, and the tool, and carrying
/// none of the set's schema or description text.
#[track_caller]
fn refused_on_decode(set: &McpDescriptorSet, reason: &str) {
    let value = serde_json::to_value(set).expect("encodes");
    let error = serde_json::from_value::<McpDescriptorSet>(value).expect_err("an inconsistent set");
    let text = error.to_string();
    assert!(text.contains("mcp-descriptor-set-invalid"), "{text}");
    assert!(text.contains(reason), "{text}");
    assert!(text.contains("crm"), "names the server: {text}");
    assert!(
        !text.contains("Searches.") && !text.contains("\"q\""),
        "carries no server text: {text}"
    );
}

#[tokio::test]
async fn a_set_that_names_one_tool_twice_is_refused_in_either_order() {
    let (set, _) = synced().await;
    let mut appended = set.clone();
    appended.descriptors.push(set.descriptors[0].clone());
    refused_on_decode(&appended, "names the tool twice");
    let mut adjacent = set.clone();
    adjacent.descriptors.insert(1, set.descriptors[0].clone());
    refused_on_decode(&adjacent, "names the tool twice");
}

#[tokio::test]
async fn a_descriptor_of_another_kind_is_refused() {
    let (mut set, _) = synced().await;
    set.descriptors[0].binding = rebound(&set.descriptors[0], |descriptor| {
        descriptor.kind = AgentToolKind::Function;
    });
    refused_on_decode(&set, "is not a remote MCP tool");
}

#[tokio::test]
async fn a_tool_id_its_server_and_name_do_not_derive_is_refused() {
    let (mut set, _) = synced().await;
    set.descriptors[0].binding = rebound(&set.descriptors[0], |descriptor| {
        descriptor.tool = AgentToolId::new("mcp.other.search").expect("a valid id");
    });
    refused_on_decode(&set, "tool id");
}

#[tokio::test]
async fn a_schema_digest_that_is_not_its_schemas_is_refused() {
    let (mut set, _) = synced().await;
    set.descriptors[0].schema_digest = AgentContentDigest::sha256_of_json(&json!({ "x": 1 }));
    refused_on_decode(&set, "schema digest");
}

#[tokio::test]
async fn inline_parameters_that_differ_from_the_input_schema_are_refused() {
    let (mut set, _) = synced().await;
    set.descriptors[0].binding = rebound(&set.descriptors[0], |descriptor| {
        descriptor.parameters = Some(json!({ "type": "object", "properties": {} }));
    });
    refused_on_decode(&set, "inline parameters");
}

#[tokio::test]
async fn absent_inline_parameters_are_allowed() {
    let (mut set, _) = synced().await;
    set.descriptors[0].binding = rebound(&set.descriptors[0], |descriptor| {
        descriptor.parameters = None;
    });
    let value = serde_json::to_value(&set).expect("encodes");
    serde_json::from_value::<McpDescriptorSet>(value)
        .expect("a schema the descriptor does not carry inline is allowed");
}

#[tokio::test]
async fn a_version_2_output_schema_digest_that_is_not_its_schemas_is_refused() {
    let (set, _) = synced().await;
    let mut wrong = set.clone();
    wrong.descriptors[0].output_schema_digest =
        Some(AgentContentDigest::sha256_of_json(&json!({ "x": 1 })));
    refused_on_decode(&wrong, "output schema digest");
    let mut missing = set.clone();
    missing.descriptors[0].output_schema = None;
    refused_on_decode(&missing, "output schema digest");
}

#[tokio::test]
async fn a_version_1_set_is_held_to_every_check_but_the_output_schema() {
    let (set, _) = synced().await;
    let mut v1 = serde_json::to_value(&set).expect("encodes");
    v1["schema_version"] = json!(1);
    for descriptor in v1["descriptors"].as_array_mut().expect("descriptors") {
        let object = descriptor.as_object_mut().expect("an object");
        for field in ["output_schema", "annotations", "description_supplied"] {
            object.remove(field);
        }
    }
    let decoded = serde_json::from_value::<McpDescriptorSet>(v1.clone())
        .expect("a version-1 set keeps its output digest with no raw schema to compare");
    assert!(decoded.descriptors[0].output_schema_digest.is_some());

    let mut duplicated = v1;
    let first: Value = duplicated["descriptors"][0].clone();
    duplicated["descriptors"]
        .as_array_mut()
        .expect("descriptors")
        .push(first);
    let error = serde_json::from_value::<McpDescriptorSet>(duplicated)
        .expect_err("a version-1 set with a duplicate is refused too");
    assert!(
        error.to_string().contains("mcp-descriptor-set-invalid"),
        "{error}"
    );
}

#[tokio::test]
async fn validate_answers_the_typed_refusal_and_the_executor_refuses_the_set() {
    let (set, binding) = synced().await;
    assert!(set.validate().is_ok());
    let mut duplicated = set.clone();
    duplicated.descriptors.push(set.descriptors[0].clone());

    let refusal = duplicated.validate().expect_err("a duplicate");
    assert_eq!(refusal.code(), "mcp-descriptor-set-invalid");

    let refused = McpDispatchToolExecutor::new(
        vec![duplicated],
        vec![binding],
        mcp_artifact_store(SharedArtifactStore::default()),
        hardened_reqwest_client(),
        Arc::new(McpAllowAllEgress),
    )
    .err()
    .expect("construction refuses a set built in code");
    assert_eq!(refused.code(), "mcp-descriptor-set-invalid");
}

#[tokio::test]
async fn every_reader_takes_the_first_copy_of_a_duplicated_tool() {
    let (set, _) = synced().await;
    // A second `search` whose schema differs, consistent with itself.
    let reshaped = json!({ "type": "object", "properties": { "other": { "type": "string" } } });
    let mut second = set.descriptors[0].clone();
    second.schema_digest = AgentContentDigest::sha256_of_json(&reshaped);
    second.binding = rebound(&second, |descriptor| {
        descriptor.parameters = Some(reshaped.clone());
    });
    second.input_schema = reshaped;
    let mut duplicated = set.clone();
    duplicated.descriptors.push(second);

    assert_eq!(
        duplicated.descriptor("search"),
        Some(&set.descriptors[0]),
        "descriptor() reads the first copy"
    );
    assert_eq!(
        mcp_descriptor_staleness(&duplicated, &set),
        McpDescriptorStaleness::Fresh,
        "the staleness check reads the first copy too"
    );
}
