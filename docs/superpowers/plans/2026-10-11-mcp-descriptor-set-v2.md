# MCP Descriptor Set Version 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the stored `McpDescriptorSet` to schema version 2 (each tool's raw output schema, annotations, and description origin) and make every decoded or executor-held set check itself — issue #85 ask 3, ask 10, and the ask-10 addendum.

**Architecture:** Three serde-defaulted fields join `McpSyncedDescriptor`, filled by the sync, absent on a version-1 set. `McpDescriptorSet` decodes through a private record and then `validate()`, which the executor also calls at construction; a failure is the new `McpRegistrationError::DescriptorSetInvalid` (`mcp-descriptor-set-invalid`). The staleness check keeps the first copy of a tool, as the other readers do.

**Tech Stack:** Rust 1.88 (MSRV), serde (`try_from`), rmcp 3.4.0 (`ToolAnnotations` read at sync only), the `rakka-agent-mcp` crate and its testkit fake server.

**Spec:** `docs/superpowers/specs/2026-10-11-mcp-descriptor-set-v2-design.md`

## Global Constraints

- MSRV is Rust 1.88: `cargo +1.88.0 check --workspace --all-targets` must pass.
- Workspace lints: `missing_docs = "warn"` and validation runs `clippy -D warnings`, so every new public item has a doc comment.
- `MCP_DESCRIPTOR_SET_SCHEMA_VERSION` = **2**. Decode reads 1 and 2 and refuses a newer one.
- `McpDescriptorSet::digest()` and `mcp_descriptor_staleness`'s result do not change.
- The new stable code is `mcp-descriptor-set-invalid`; a refusal's `reason` is a fixed phrase, never server-chosen text.
- Checks run on every decode, version 1 and 2, and at executor construction (the owner's ruling).
- rmcp types never become the stored format: annotations are stored as the crate-owned `McpToolAnnotations`.
- This machine's rule: `cargo clean` between tasks (never while a cargo command is running); run tests per crate.
- Commit subjects are imperative sentences with no prefix; bodies explain the mechanism; every commit ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Branch: `mcp-descriptor-set-v2` (created off `a3e5c10`; the spec is committed as `b373c25`).

## Review Focus

1. A version-1 set that carries an `output_schema_digest` but, being version 1, no raw `output_schema` still decodes and validates — check 6 applies to version 2 only. Tested in Task 2.
2. An annotation title longer than the bound, in multi-byte characters, is cut on a character boundary rather than panicking or overrunning. Tested in Task 1.
3. Two copies of one tool that are byte-for-byte identical are still a duplicate and refused. Tested in Task 2.
4. A refusal's text names the check, the server, and the tool, and never carries the set's schema or description text. Tested in Task 2.
5. A hand-authored set that omits inline `parameters` for a small schema still decodes (absent parameters are allowed). Tested in Task 2.

---

## File Structure

| File | Change | Responsibility |
| --- | --- | --- |
| `crates/rakka-agent-mcp/src/sync.rs` | Modify | `McpToolAnnotations`; the three fields; the sync filling them; the title echo rule; `validate()`; the decoding record; staleness keeps the first copy |
| `crates/rakka-agent-mcp/src/binding.rs` | Modify | `DescriptorSetInvalid` variant, its code and `Display`; `derived_tool_id` shared by `tool_id` and `validate` |
| `crates/rakka-agent-mcp/src/executor.rs` | Modify | `build` validates each set; its unit-test set literal gains the three fields |
| `crates/rakka-agent-mcp/src/lib.rs` | Modify | export `McpToolAnnotations` |
| `crates/rakka-agent-mcp/tests/descriptor_sync.rs` | Modify | version-2 sync proofs |
| `crates/rakka-agent-mcp/tests/sync_credential_echo.rs` | Modify | the title echo proof |
| `crates/rakka-agent-mcp/tests/descriptor_set_validation.rs` | Create | the self-check proofs |
| `crates/rakka-agent/tests/compatibility_currency.rs`, `docs/rakka-compatibility.md`, `CHANGELOG.md`, the spec's status line | Modify | Task 3 |

---

### Task 1: Version 2 keeps annotations, the output schema, and the description origin

**Files:**
- Modify: `crates/rakka-agent-mcp/src/sync.rs` — `McpSyncedDescriptor` (top of file), `MCP_DESCRIPTOR_SET_SCHEMA_VERSION`, `McpSyncError::CredentialEchoed`'s field doc, `synced_descriptor` (search `fn synced_descriptor(`)
- Modify: `crates/rakka-agent-mcp/src/lib.rs` (`pub use sync::{..}`)
- Modify: `crates/rakka-agent-mcp/src/executor.rs` (the unit-test `fn set(` literal, search `McpSyncedDescriptor {`)
- Test: `crates/rakka-agent-mcp/tests/descriptor_sync.rs`, `crates/rakka-agent-mcp/tests/sync_credential_echo.rs`

**Interfaces:**
- Produces: `pub struct McpToolAnnotations { pub title: Option<String>, pub read_only_hint: Option<bool>, pub destructive_hint: Option<bool>, pub idempotent_hint: Option<bool>, pub open_world_hint: Option<bool> }` (`Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize`); on `McpSyncedDescriptor`: `pub output_schema: Option<Value>`, `pub annotations: Option<McpToolAnnotations>`, `pub description_supplied: Option<bool>`; `MCP_DESCRIPTOR_SET_SCHEMA_VERSION == 2`. Task 2's `validate()` reads `output_schema` and `schema_version`.

- [ ] **Step 1: Write the failing tests**

In `crates/rakka-agent-mcp/tests/descriptor_sync.rs`, add `AgentContentDigest, AGENT_TOOL_DESCRIPTION_MAX_LENGTH` to the `rakka_agent::{..}` import and `McpToolAnnotations` to the `rakka_agent_mcp::{..}` import, then append:

```rust
#[tokio::test]
async fn a_synced_set_is_version_2_and_keeps_annotations_the_output_schema_and_the_description_origin(
) {
    let output = json!({ "type": "object", "properties": { "id": { "type": "string" } } });
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(
                tool("search")
                    .with_annotations(
                        ToolAnnotations::with_title("Search the CRM")
                            .read_only(true)
                            .open_world(false),
                    )
                    .with_output_schema(output.clone()),
            )
            .with_tool(FakeTool::text("update", "", schema(1), "ok")),
    )
    .await;
    let set = sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");

    assert_eq!(set.schema_version, 2);
    let search = set.descriptor("search").expect("present");
    assert_eq!(
        search.annotations,
        Some(McpToolAnnotations {
            title: Some("Search the CRM".to_string()),
            read_only_hint: Some(true),
            destructive_hint: None,
            idempotent_hint: None,
            open_world_hint: Some(false),
        })
    );
    assert_eq!(search.output_schema.as_ref(), Some(&output));
    assert_eq!(
        search.output_schema_digest,
        Some(AgentContentDigest::sha256_of_json(&output))
    );
    assert_eq!(search.description_supplied, Some(true));

    let update = set.descriptor("update").expect("present");
    assert_eq!(update.binding.descriptor().description, "MCP tool update on crm");
    assert_eq!(
        update.description_supplied,
        Some(false),
        "an empty description is the fallback, and the set says so"
    );
    assert_eq!(update.annotations, None);
    assert_eq!(update.output_schema, None);

    let decoded: McpDescriptorSet =
        serde_json::from_str(&serde_json::to_string(&set).expect("encodes")).expect("decodes");
    assert_eq!(decoded, set);
}

#[tokio::test]
async fn a_version_1_set_still_decodes_with_the_new_fields_absent_and_the_same_digest() {
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(
                tool("search")
                    .with_annotations(ToolAnnotations::with_title("Search the CRM"))
                    .with_output_schema(json!({ "type": "object" })),
            )
            .with_tool(tool("update")),
    )
    .await;
    let set = sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");

    // The shape a build before version 2 wrote.
    let mut v1 = serde_json::to_value(&set).expect("encodes");
    v1["schema_version"] = json!(1);
    for descriptor in v1["descriptors"].as_array_mut().expect("descriptors") {
        let object = descriptor.as_object_mut().expect("an object");
        for field in ["output_schema", "annotations", "description_supplied"] {
            object.remove(field);
        }
    }
    let decoded: McpDescriptorSet =
        serde_json::from_value(v1).expect("a version-1 set decodes");

    assert_eq!(decoded.schema_version, 1);
    assert!(decoded.descriptors.iter().all(|descriptor| {
        descriptor.output_schema.is_none()
            && descriptor.annotations.is_none()
            && descriptor.description_supplied.is_none()
    }));
    assert_eq!(
        decoded.digest(),
        set.digest(),
        "version 2 pins the release digest version 1 pinned"
    );
}

#[tokio::test]
async fn an_annotation_title_over_the_bound_is_cut_on_a_character_boundary() {
    let long = "é".repeat(AGENT_TOOL_DESCRIPTION_MAX_LENGTH);
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(tool("search").with_annotations(ToolAnnotations::with_title(long.clone())))
            .with_tool(tool("update")),
    )
    .await;
    let set = sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs");
    let title = set
        .descriptor("search")
        .and_then(|descriptor| descriptor.annotations.as_ref())
        .and_then(|annotations| annotations.title.clone())
        .expect("a title");
    assert!(title.len() <= AGENT_TOOL_DESCRIPTION_MAX_LENGTH, "{}", title.len());
    assert!(!title.is_empty() && long.starts_with(&title));
}
```

In `crates/rakka-agent-mcp/tests/sync_credential_echo.rs`, add `use rmcp::model::ToolAnnotations;` and append:

```rust
#[tokio::test]
async fn an_annotation_title_that_carries_the_credential_is_refused() {
    let credential = AgentEphemeralCredential::bearer_token(TOKEN);
    let server = FakeMcpServer::new().with_tool(
        search("Searches.", json!({"type":"object"}))
            .with_annotations(ToolAnnotations::with_title(format!("SERVER-WORDS {TOKEN}"))),
    );
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "annotations title",
        Some("search"),
        TOKEN,
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test descriptor_sync --test sync_credential_echo`
Expected: FAIL to compile — `unresolved import rakka_agent_mcp::McpToolAnnotations` and `no field output_schema on type &McpSyncedDescriptor`.

- [ ] **Step 3: Add the type and the fields**

In `crates/rakka-agent-mcp/src/sync.rs`, before `/// One tool as this adapter stored it at publish time.`:

```rust
/// The tool annotations an MCP server reported, as this adapter stores them.
///
/// Owned by this crate rather than rmcp's `ToolAnnotations`, so the stored
/// format never moves with the SDK's serde shape. Every field is the server's
/// claim, never a declaration: a hint is evidence only through the sync's own
/// hint rule, and the title is server-chosen text, cut at
/// [`AGENT_TOOL_DESCRIPTION_MAX_LENGTH`] and refused when it carries the
/// credential the sync sent.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct McpToolAnnotations {
    /// A human-readable title for the tool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// The server's `readOnlyHint`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only_hint: Option<bool>,
    /// The server's `destructiveHint`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destructive_hint: Option<bool>,
    /// The server's `idempotentHint`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotent_hint: Option<bool>,
    /// The server's `openWorldHint`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_world_hint: Option<bool>,
}

```

In `pub struct McpSyncedDescriptor`, after `output_schema_digest`:

```rust
    /// The server's raw output schema, when it declared one. Absent on a
    /// version-1 set, which stored only its digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    /// The tool annotations the server reported, when it reported any. Absent
    /// on a version-1 set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<McpToolAnnotations>,
    /// Whether the descriptor's description is the server's own (`true`) or
    /// this adapter's fallback, `MCP tool {tool} on {server}` (`false`).
    /// `None` on a version-1 set, which did not record it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description_supplied: Option<bool>,
```

Replace the version constant and its doc:

```rust
/// The schema version of the stored [`McpDescriptorSet`] shape this build
/// writes, and the newest it reads. Version 2 added each tool's raw
/// [`McpSyncedDescriptor::output_schema`], its
/// [`McpSyncedDescriptor::annotations`], and
/// [`McpSyncedDescriptor::description_supplied`]; a version-1 set still
/// decodes, with all three absent.
pub const MCP_DESCRIPTOR_SET_SCHEMA_VERSION: u32 = 2;
```

In `McpSyncError::CredentialEchoed`, change the `field` doc to: `/// Which text: \`server name\`, \`description\`, \`annotations title\`, \`input schema\`, or \`output schema\`.`

- [ ] **Step 4: Fill them in the sync**

In `synced_descriptor`, after the `output schema` echo check (the block ending `return Err(echoed("output schema"));`), add:

```rust
    if listed
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.title.as_deref())
        .is_some_and(|title| carries(title, secrets))
    {
        return Err(echoed("annotations title"));
    }
```

Before `let fallback = format!("MCP tool {tool} on {server}");`, add:

```rust
    let description_supplied = listed
        .description
        .as_deref()
        .is_some_and(|description| !description.is_empty());
    let annotations = listed
        .annotations
        .as_ref()
        .map(|annotations| McpToolAnnotations {
            title: annotations.title.as_deref().map(bounded_description),
            read_only_hint: annotations.read_only_hint,
            destructive_hint: annotations.destructive_hint,
            idempotent_hint: annotations.idempotent_hint,
            open_world_hint: annotations.open_world_hint,
        });
```

and replace the closing construction:

```rust
    let output_schema_digest = output_schema
        .as_ref()
        .map(AgentContentDigest::sha256_of_json);
    Ok(McpSyncedDescriptor {
        tool: tool.to_string(),
        binding: derived,
        schema_digest: AgentContentDigest::sha256_of_json(&input_schema),
        input_schema,
        output_schema_digest,
        input_schema_artifact: None,
    })
```

with:

```rust
    let output_schema_digest = output_schema
        .as_ref()
        .map(AgentContentDigest::sha256_of_json);
    Ok(McpSyncedDescriptor {
        tool: tool.to_string(),
        binding: derived,
        schema_digest: AgentContentDigest::sha256_of_json(&input_schema),
        input_schema,
        output_schema_digest,
        input_schema_artifact: None,
        output_schema,
        annotations,
        description_supplied: Some(description_supplied),
    })
```

In `crates/rakka-agent-mcp/src/lib.rs`, add `McpToolAnnotations` to the `pub use sync::{..}` list. In `crates/rakka-agent-mcp/src/executor.rs`'s unit-test `fn set(`, add to the `McpSyncedDescriptor { .. }` literal after `input_schema_artifact: None,`:

```rust
                        output_schema: None,
                        annotations: None,
                        description_supplied: Some(true),
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo fmt -p rakka-agent-mcp && cargo test -p rakka-agent-mcp --features testkit`
Expected: PASS, including the four new tests and the existing `sync_returns_only_the_allow_listed_tools_as_prefixed_bindings_with_digests` (its newer-version case now writes version 3).

- [ ] **Step 6: Commit**

```bash
git add crates/rakka-agent-mcp/src/sync.rs crates/rakka-agent-mcp/src/lib.rs crates/rakka-agent-mcp/src/executor.rs crates/rakka-agent-mcp/tests/descriptor_sync.rs crates/rakka-agent-mcp/tests/sync_credential_echo.rs
git commit -m "Keep each MCP tool's annotations, output schema, and description origin

The stored descriptor set moves to schema version 2. Each descriptor now
keeps the server's raw output schema beside its digest, its annotations
as the crate-owned McpToolAnnotations (rmcp's serde shape never becomes
the stored format), and description_supplied, which says whether the
description is the server's or this adapter's fallback. A version-1 set
decodes with all three absent. The annotation title is server-chosen
text that now becomes release data, so it is cut at the description
bound and refused under mcp-descriptor-credential-echoed when it carries
the credential the sync sent. The release digest and staleness are
unchanged, so a set re-synced into version 2 pins the digest it pinned.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: A descriptor set checks itself, and the readers agree

**Files:**
- Modify: `crates/rakka-agent-mcp/src/binding.rs` — `McpRegistrationError` (variant, `code()`, `Display`), `McpServerBinding::tool_id` (search `pub fn tool_id(`)
- Modify: `crates/rakka-agent-mcp/src/sync.rs` — `McpDescriptorSet` (derive attributes, `validate`), a private decoding record, `shapes`
- Modify: `crates/rakka-agent-mcp/src/executor.rs` — `build` (search `for set in descriptors {`)
- Create: `crates/rakka-agent-mcp/tests/descriptor_set_validation.rs`

**Interfaces:**
- Consumes: `McpSyncedDescriptor::output_schema` and `McpDescriptorSet::schema_version` (Task 1).
- Produces: `McpRegistrationError::DescriptorSetInvalid { server: String, tool: Option<String>, reason: &'static str }` (code `mcp-descriptor-set-invalid`); `pub fn validate(&self) -> Result<(), McpRegistrationError>` on `McpDescriptorSet`; `pub(crate) fn derived_tool_id(server: &McpServerId, tool: &str) -> String` in `binding.rs`.

- [ ] **Step 1: Write the failing tests**

Create `crates/rakka-agent-mcp/tests/descriptor_set_validation.rs`:

```rust
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
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::Idempotent)),
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
            .with_tool(FakeTool::text("update", "Updates.", json!({ "type": "object" }), "ok")),
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
    let error =
        serde_json::from_value::<McpDescriptorSet>(value).expect_err("an inconsistent set");
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
    assert!(error.to_string().contains("mcp-descriptor-set-invalid"), "{error}");
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test descriptor_set_validation`
Expected: FAIL to compile — `no method named validate found for struct McpDescriptorSet`.

- [ ] **Step 3: The refusal and the shared tool-id derivation**

In `crates/rakka-agent-mcp/src/binding.rs`, add to `pub enum McpRegistrationError` after `InvalidServerId { .. }`:

```rust
    /// A descriptor set disagrees with itself: a tool named twice, or a
    /// descriptor whose kind, tool id, schema digests, or inline parameters
    /// do not match what its own fields derive.
    DescriptorSetInvalid {
        /// The server the set belongs to.
        server: String,
        /// The tool the check failed on.
        tool: Option<String>,
        /// Which check failed: a fixed phrase, never the set's own text.
        reason: &'static str,
    },
```

In `code()`, add a first arm: `Self::DescriptorSetInvalid { .. } => "mcp-descriptor-set-invalid",`. In `Display`, after the `InvalidServerId` arm:

```rust
            Self::DescriptorSetInvalid {
                server,
                tool: Some(tool),
                reason,
            } => write!(
                f,
                "the MCP server {server}'s descriptor set is invalid at the tool {tool}: {reason}"
            ),
            Self::DescriptorSetInvalid {
                server,
                tool: None,
                reason,
            } => write!(f, "the MCP server {server}'s descriptor set is invalid: {reason}"),
```

Add, near `MCP_TOOL_ID_PREFIX`:

```rust
/// The tool id one server's tool is registered under: `mcp.<server>.<tool>`.
///
/// The one derivation [`McpServerBinding::tool_id`] and a descriptor set's
/// own check both use.
pub(crate) fn derived_tool_id(server: &McpServerId, tool: &str) -> String {
    format!("{MCP_TOOL_ID_PREFIX}{server}.{tool}")
}
```

and in `McpServerBinding::tool_id` replace `let server = &self.server_id;` and `let candidate = format!("{MCP_TOOL_ID_PREFIX}{server}.{tool}");` with `let candidate = derived_tool_id(&self.server_id, tool);`.

- [ ] **Step 4: `validate()`, the decoding record, and the first-copy staleness**

In `crates/rakka-agent-mcp/src/sync.rs`:

Add `derived_tool_id` to the `crate::binding::{..}` import. Change the derive line above `pub struct McpDescriptorSet` to:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "McpDescriptorSetRecord")]
```

remove `#[serde(deserialize_with = "known_set_schema_version")]` from its `schema_version` field, extend the struct's doc with `/// Every decode checks the set against itself ([`Self::validate`]), whatever its version.`, and add after `impl McpDescriptorSet`'s `descriptor` method:

```rust
    /// Refuses a set that disagrees with itself.
    ///
    /// A set is release data a client may author, so nothing about it is
    /// trusted for having decoded: each tool is named once, and each
    /// descriptor is a remote MCP tool registered under the id its server and
    /// name derive, whose schema digest is its input schema's, whose inline
    /// parameters (when it carries any) are its input schema, and — on a
    /// version-2 set, which stores the raw output schema — whose output
    /// schema digest is its output schema's. Decode runs this on every set,
    /// and so does executor construction.
    ///
    /// # Errors
    ///
    /// [`McpRegistrationError::DescriptorSetInvalid`], stable code
    /// `mcp-descriptor-set-invalid`, at the first check that fails.
    pub fn validate(&self) -> Result<(), McpRegistrationError> {
        let invalid = |tool: &str, reason: &'static str| McpRegistrationError::DescriptorSetInvalid {
            server: self.server_id.to_string(),
            tool: Some(tool.to_string()),
            reason,
        };
        let mut named = std::collections::BTreeSet::new();
        for descriptor in &self.descriptors {
            let tool = descriptor.tool.as_str();
            if !named.insert(tool) {
                return Err(invalid(tool, "the set names the tool twice"));
            }
            let declared = descriptor.binding.descriptor();
            if declared.kind != AgentToolKind::RemoteMcp {
                return Err(invalid(tool, "the descriptor is not a remote MCP tool"));
            }
            if declared.tool.as_str() != derived_tool_id(&self.server_id, tool) {
                return Err(invalid(
                    tool,
                    "the tool id is not the one its server and name derive",
                ));
            }
            if descriptor.schema_digest != AgentContentDigest::sha256_of_json(&descriptor.input_schema) {
                return Err(invalid(
                    tool,
                    "the schema digest is not the digest of the input schema",
                ));
            }
            if declared
                .parameters
                .as_ref()
                .is_some_and(|parameters| parameters != &descriptor.input_schema)
            {
                return Err(invalid(
                    tool,
                    "the inline parameters differ from the input schema",
                ));
            }
            if self.schema_version >= 2
                && descriptor.output_schema_digest
                    != descriptor
                        .output_schema
                        .as_ref()
                        .map(AgentContentDigest::sha256_of_json)
            {
                return Err(invalid(
                    tool,
                    "the output schema digest is not the digest of the output schema",
                ));
            }
        }
        Ok(())
    }
```

Add, after `fn known_set_schema_version`:

```rust
/// A descriptor set as it is read, before it checks itself.
#[derive(Deserialize)]
struct McpDescriptorSetRecord {
    #[serde(deserialize_with = "known_set_schema_version")]
    schema_version: u32,
    server_id: McpServerId,
    server_name: String,
    protocol_version: String,
    synced_at: AgentTimestampMillis,
    descriptors: Vec<McpSyncedDescriptor>,
}

impl TryFrom<McpDescriptorSetRecord> for McpDescriptorSet {
    type Error = String;

    fn try_from(record: McpDescriptorSetRecord) -> Result<Self, Self::Error> {
        let set = Self {
            schema_version: record.schema_version,
            server_id: record.server_id,
            server_name: record.server_name,
            protocol_version: record.protocol_version,
            synced_at: record.synced_at,
            descriptors: record.descriptors,
        };
        set.validate()
            .map_err(|refusal| format!("{}: {refusal}", refusal.code()))?;
        Ok(set)
    }
}
```

Replace `shapes`' body (the `set.descriptors.iter().map(..).collect()`) with:

```rust
    // The first copy of a tool, as `descriptor()` and the executor's routing
    // table read it. A validated set holds one copy; an unvalidated one built
    // in code is still read alike by every reader.
    let mut shapes = BTreeMap::new();
    for descriptor in &set.descriptors {
        shapes.entry(descriptor.tool.as_str()).or_insert((
            &descriptor.schema_digest,
            descriptor.output_schema_digest.as_ref(),
        ));
    }
    shapes
```

and change its doc to `/// The comparable shape of every tool in a set: the two schema digests of its first copy.`

- [ ] **Step 5: The executor validates each set**

In `crates/rakka-agent-mcp/src/executor.rs`'s `build`, change `for set in descriptors {` to:

```rust
        for set in descriptors {
            // A set built in code never went through decode's check.
            set.validate()?;
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo fmt -p rakka-agent-mcp && cargo test -p rakka-agent-mcp --features testkit`
Expected: PASS — the eleven new tests and every existing one (the executor's unit-test sets and every synced set are consistent).
Run: `cargo test -p rakka-agent --test mcp_client_dispatch`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent-mcp/src/binding.rs crates/rakka-agent-mcp/src/sync.rs crates/rakka-agent-mcp/src/executor.rs crates/rakka-agent-mcp/tests/descriptor_set_validation.rs
git commit -m "Check a stored MCP descriptor set against itself

A set is release data a client may author, and decode checked only its
version, server id, and bindings. McpDescriptorSet::validate now refuses
a tool named twice, a descriptor that is not a remote MCP tool or whose
tool id is not the one its server and name derive (derived_tool_id, the
derivation tool_id uses), a schema digest that is not its input schema's,
inline parameters that differ from the input schema, and on a version-2
set an output schema digest that is not its output schema's — under the
new McpRegistrationError::DescriptorSetInvalid
(mcp-descriptor-set-invalid). It runs on every decode of either version,
through a private record the set is tried from, and at executor
construction, for a set built in code. The staleness check now reads the
first copy of a tool, as descriptor() and the routing table do.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Documentation

**Files:**
- Modify: `crates/rakka-agent/tests/compatibility_currency.rs` (`fn declared_schema_versions`)
- Modify: `docs/rakka-compatibility.md` (the schema-version table, the paragraph after it, and a new bullet before `- Issue #85 asks 7, 14, and 15 (`)
- Modify: `CHANGELOG.md` (`### Fixed`, before `### Security`)
- Modify: `docs/superpowers/specs/2026-10-11-mcp-descriptor-set-v2-design.md` (status line)

- [ ] **Step 1: The schema-version row, test first**

In `crates/rakka-agent/tests/compatibility_currency.rs`, before `declared` is returned in `declared_schema_versions`:

```rust
    declared.insert(
        "MCP_DESCRIPTOR_SET_SCHEMA_VERSION".to_string(),
        (
            "rakka-agent-mcp".to_string(),
            rakka_agent_mcp::MCP_DESCRIPTOR_SET_SCHEMA_VERSION,
        ),
    );
```

Run: `cargo test -p rakka-agent --features otel --test compatibility_currency`
Expected: FAIL — `every_durable_record_schema_version_is_documented_and_current` names the undocumented `MCP_DESCRIPTOR_SET_SCHEMA_VERSION`.

Then in `docs/rakka-compatibility.md`, add after the `| \`CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION\` | \`rakka-agent-workflow\` | 1 |` row:

```markdown
| `MCP_DESCRIPTOR_SET_SCHEMA_VERSION` | `rakka-agent-mcp` | 2 |
```

replace `The one record above version 1, and what its bump requires of a rollout:` with `The records above version 1, and what each bump requires of a rollout:`, and add after that list's last bullet:

```markdown
- The stored MCP descriptor set (`McpDescriptorSet`) is at schema version **2** (issue #85 ask 3). A build before version 2 refuses a version-2 set, so a deployment upgrades every reader of its stored sets before it publishes from a build that writes version 2; a version-1 set keeps decoding.
```

Run the test again. Expected: PASS.

- [ ] **Step 2: The compatibility bullet**

Insert before the `- Issue #85 asks 7, 14, and 15 (` bullet:

```markdown
- Issue #85 ask 3, ask 10, and the ask-10 addendum ("descriptor set version 2") move `McpDescriptorSet` to schema version 2, add one stable MCP code, and make every decode check the set against itself. **Version 2** adds three fields to each `McpSyncedDescriptor`, all `Option` and serde-defaulted, absent on a version-1 set, which still decodes: `output_schema` (the server's raw output schema beside the digest that was already stored), `annotations` (the new `McpToolAnnotations` — `title`, `read_only_hint`, `destructive_hint`, `idempotent_hint`, `open_world_hint` — owned by this crate so rmcp's serde shape is never the stored format), and `description_supplied` (`true` for the server's own description, `false` for the fallback `MCP tool {tool} on {server}`). The annotation title is server-chosen text that now becomes release data: the sync cuts it at `AGENT_TOOL_DESCRIPTION_MAX_LENGTH` on a character boundary and refuses one that carries the credential it sent, under `mcp-descriptor-credential-echoed` with the field `annotations title`. `McpDescriptorSet::digest()` and `mcp_descriptor_staleness` are unchanged, so a set re-synced into version 2 pins the release digest it pinned as version 1. **The self-check:** `McpDescriptorSet::validate()` refuses a tool named twice, a descriptor whose binding is not `AgentToolKind::RemoteMcp`, a tool id other than `mcp.<server>.<tool>`, a `schema_digest` that is not the SHA-256 of `input_schema`, inline `parameters` that are present and differ from `input_schema` (absent ones are allowed), and on a version-2 set an `output_schema_digest` that is not the SHA-256 of `output_schema` (both present and agreeing, or both absent), as the new `McpRegistrationError::DescriptorSetInvalid { server, tool, reason }` under **`mcp-descriptor-set-invalid`**, the eighteenth MCP code; `reason` is a fixed phrase, never the set's own text. It runs on **every decode, whatever the version** — the serde error leads with the code — and at `McpDispatchToolExecutor` construction, so a set built in code is held to it too. **A stored set that fails a check stops decoding**: a release a deployment published before it checked duplicate tool names itself (rakka.host's A16–A17 window) may need repair before this build reads it. `mcp_descriptor_staleness` now reads the first copy of a duplicated tool, as `descriptor()` and the executor's routing table already did. **Source-breaking, compile-time only:** `McpSyncedDescriptor` is not `#[non_exhaustive]` and gains three public fields, so a struct literal of it (rakka.host's test kit has one) adds `output_schema: None, annotations: None, description_supplied: None`.
```

- [ ] **Step 3: The changelog and the spec status**

Add to `### Fixed`, before `### Security`:

```markdown
- A stored MCP descriptor set kept only the digest of a tool's output schema, none of its annotations, and no record of whether its description was the server's or this adapter's fallback, and it decoded without checking itself — a set naming one tool twice, or whose digest, tool id, kind, or inline parameters disagreed with its own schema, was accepted, and its readers disagreed about which copy of a tool won. `McpDescriptorSet` is now schema version 2, keeping each tool's raw output schema, its annotations (`McpToolAnnotations`), and `description_supplied`, and every decode and executor construction checks the set against itself under the new `mcp-descriptor-set-invalid` (issue #85, ask 3, ask 10, and the ask-10 addendum).
```

In the spec, replace `Status: design, 2026-10-11.` with `Status: implemented, 2026-10-11.`

- [ ] **Step 4: Commit**

```bash
git add crates/rakka-agent/tests/compatibility_currency.rs docs/rakka-compatibility.md CHANGELOG.md docs/superpowers/specs/2026-10-11-mcp-descriptor-set-v2-design.md
git commit -m "Record descriptor set version 2 in the compatibility document and the changelog

The schema-version table gains MCP_DESCRIPTOR_SET_SCHEMA_VERSION at 2,
held by compatibility_currency.rs, with its rollout rule beside the run
state's. The compatibility document registers the three version-2 fields,
McpToolAnnotations, the annotation title's bound and echo rule, the
unchanged digest and staleness, validate() and mcp-descriptor-set-invalid
on every decode and at executor construction, the cost of strict decode,
and the McpSyncedDescriptor struct-literal break; the changelog records
the fix and the spec is marked implemented.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Validation and the pull request

- [ ] **Step 1: Full validation.** Run in the background with `SCRATCH` set to the session's scratchpad directory, wait for the notification, and read the log:

```bash
scripts/validate.sh > "$SCRATCH/validate-descriptor-set-v2.log" 2>&1; echo "VALIDATE_EXIT=$?" >> "$SCRATCH/validate-descriptor-set-v2.log"
```

Expected: `VALIDATE_EXIT=0`, no `warning:` lines. A failure only in `examples/generated-contracts` with `ask timed out` is the known load flake: check for another project's `cargo test`, wait, and re-run.

- [ ] **Step 2: Cold MSRV check.**

```bash
rm -rf target/msrv88-v2 && CARGO_TARGET_DIR=target/msrv88-v2 cargo +1.88.0 check --workspace --all-targets; echo "MSRV_EXIT=$?"; rm -rf target/msrv88-v2
```

Expected: `MSRV_EXIT=0`.

- [ ] **Step 3: Push and open the PR against `main`** after the final review: title `Store MCP descriptor set version 2 and check every set against itself`; the body names the version bump and its rollout rule, the strict decode and its cost, the `McpSyncedDescriptor` struct-literal break, the tests with their pre-fix failures, and the validation results, ending with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
