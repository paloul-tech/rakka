# MCP Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `rakka-agent-mcp` a public tool listing (`list_mcp_tools`), a sync refusal that names every missing tool at once, the server's self-reported version on the session, the stored set and the listing, and an rmcp-free `FakeTool::with_annotations` — issue #85 asks 1, 2 and 4.

**Architecture:** The listing is a new module, `listing.rs`, that runs the sync's own connection path (`connect`, one bounded `tools/list`, the session closed) and shares the sync's bounding and echo helpers, made `pub(crate)` in `sync.rs`. The binding gains `validate_endpoint()`, the connection half of `validate()`. The server version rides `McpClientSession` from the handshake into both the set (an optional field on schema version 2, no bump) and the listing. Ask 2 is a two-pass `synced_descriptors` and one new `McpSyncError` variant.

**Tech Stack:** Rust (MSRV 1.88, toolchain stable), rmcp 3.4.0, serde/serde_json, tokio; tests against the crate's in-process fake MCP server (`testkit` feature).

**Spec:** `docs/superpowers/specs/2026-10-11-mcp-discovery-design.md`

## Global Constraints

- MSRV is Rust 1.88: `cargo +1.88.0 check --workspace --all-targets` must pass on a cold target.
- Workspace lints: `missing_docs` (every public item documented), `clippy -D warnings` with `--all-targets --all-features`, `unsafe_code = "forbid"`.
- No error code is added or changes meaning. Codes used: `mcp-binding-invalid`, `mcp-descriptor-sync-failed`, `mcp-descriptor-schema-too-large`, `mcp-descriptor-credential-echoed`, `mcp-peer-agent-channel-refused`, `mcp-protocol-unsupported`, and the host's own egress code.
- `MCP_DESCRIPTOR_SET_SCHEMA_VERSION` stays **2**. `server_version` is `#[serde(default, skip_serializing_if = "Option::is_none")]`, not in `digest()`, `validate()`, or `mcp_descriptor_staleness`.
- The server's version is cut at `MCP_SERVER_NAME_MAX_BYTES` (256) on a character boundary, as the name is; an absent `server_info` or an empty version reads as `None`.
- A listing stores nothing and mints no binding. It ignores the binding's allow-list.
- A listed tool over `MCP_DESCRIPTOR_SCHEMA_MAX_BYTES` or whose name cannot form a tool id is **marked** (`unusable`), never a refusal; its four schema fields are `None`. `NameInvalid` wins over `SchemaTooLarge`. A `NameInvalid` name is cut at `AGENT_IDENTITY_MAX_LENGTH` (256).
- An echoed credential anywhere in the listing's server text refuses the whole listing (`McpSyncError::CredentialEchoed`); a `tool name` echo carries `tool: None`.
- `McpServerBinding::validate()` keeps every check and its order.
- No secret in any error text, log, or returned value.
- Never publish anything; commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- **A server that lists no tools at all**: a listing answers `Ok` with an empty `tools`, not a refusal (Task 4, `a_server_with_no_tools_lists_as_empty`).
- **A credential-free listing of text that happens to contain a token-like string**: judges nothing and lists (Task 5, `the_same_text_lists_when_it_is_not_the_credential`).
- **An empty server version**: reads as `None` in both the set and the listing (Task 2 `an_empty_server_version_reads_as_none`; Task 4 asserts the listing's).
- **A child-process binding handed to the HTTP listing**: refused `mcp-descriptor-sync-failed` before any connection, never a panic (Task 4, `a_child_process_binding_is_refused_as_no_http_endpoint`).
- **A tool name at the tool-id length boundary**: a name making `mcp.crm.<name>` exactly 256 bytes is bindable and returned whole; one byte longer is `NameInvalid` and still returned whole (≤ 256) (Task 4, `a_name_at_the_tool_id_boundary_is_bindable_and_one_byte_more_is_not`).

---

## File Structure

- `crates/rakka-agent-mcp/src/binding.rs` — `validate_endpoint()`; the URL and version checks become helpers shared with `validate()`.
- `crates/rakka-agent-mcp/src/client.rs` — `McpClientSession` keeps `server_version`; the session doc names the listing as a public way to reach a server.
- `crates/rakka-agent-mcp/src/sync.rs` — `McpDescriptorSet.server_version`; `McpSyncError::ToolsNotListed`; helpers made `pub(crate)` for the listing (`cut_at`, `bounded_server_name`, `bounded_description`, `stored_annotations`, `pages_unended`, `sync_deadline`, `timed_out`, `carries`, `value_carries`, `judge_server_identity`).
- `crates/rakka-agent-mcp/src/listing.rs` (new) — `list_mcp_tools`, `list_mcp_tools_within`, `McpToolListing`, `McpListedTool`, `McpListedToolRefusal`.
- `crates/rakka-agent-mcp/src/testkit.rs` — `FakeMcpServer::with_server_version`; generic `FakeTool::with_annotations`; `From<McpToolAnnotations> for ToolAnnotations`.
- `crates/rakka-agent-mcp/src/executor.rs` — the unit-test set literal gains `server_version: None`.
- `crates/rakka-agent-mcp/src/lib.rs` — module map and exports.
- Tests: `tests/binding.rs`, `tests/descriptor_sync.rs`, `tests/sync_credential_echo.rs`, `tests/fake_server.rs`, new `tests/tool_listing.rs`, new `tests/listing_credential_echo.rs`.
- Docs: `docs/rakka-compatibility.md`, `CHANGELOG.md`, `docs/rakka-agents.md` (crate row), spec status line.

Test command shape throughout: `cargo test -p rakka-agent-mcp --features testkit --test <file>` (integration) and `cargo test -p rakka-agent-mcp --features testkit --lib` (unit).

---

### Task 1: `McpServerBinding::validate_endpoint`

**Files:**
- Modify: `crates/rakka-agent-mcp/src/binding.rs` (the `validate` method, ~line 433-520)
- Test: `crates/rakka-agent-mcp/tests/binding.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `pub fn validate_endpoint(&self) -> Result<(), McpRegistrationError>` on `McpServerBinding` (Task 4 calls it first thing).

- [ ] **Step 1: Write the failing test** — append to `crates/rakka-agent-mcp/tests/binding.rs`:

```rust
#[test]
fn validate_endpoint_reads_the_connection_half_and_ignores_the_allow_list() {
    let tool_less = McpServerBinding::streamable_http(server("s"), "https://h/mcp");
    tool_less
        .validate_endpoint()
        .expect("a binding with no tools has a valid endpoint");
    assert!(
        matches!(
            tool_less.validate().expect_err("validate still needs a tool"),
            McpRegistrationError::NoTools { .. }
        ),
        "validate keeps its allow-list check"
    );

    let bad_url = McpServerBinding::streamable_http(server("s"), "ftp://h/mcp");
    assert!(matches!(
        bad_url.validate_endpoint().expect_err("the URL rule"),
        McpRegistrationError::InvalidUrl { .. }
    ));

    let versions = |list: Vec<&str>| {
        McpServerBinding::streamable_http(server("s"), "https://h/mcp")
            .with_protocol_versions(list.into_iter().map(str::to_string).collect())
    };
    assert!(matches!(
        versions(vec![]).validate_endpoint().expect_err("empty"),
        McpRegistrationError::ProtocolVersionsEmpty { .. }
    ));
    assert!(matches!(
        versions(vec!["2026-7-28"]).validate_endpoint().expect_err("malformed"),
        McpRegistrationError::ProtocolVersionInvalid { .. }
    ));
    assert!(matches!(
        versions(vec!["2099-01-01"]).validate_endpoint().expect_err("unknown"),
        McpRegistrationError::ProtocolVersionUnknown { .. }
    ));

    let child = McpServerBinding::child_process(server("s"), launch_spec());
    child
        .clone()
        .validate_endpoint()
        .expect("a child-process endpoint with no credential");
    assert!(matches!(
        child
            .with_credential_binding(credential())
            .validate_endpoint()
            .expect_err("a child process carries no credential"),
        McpRegistrationError::CredentialOnChildProcess { .. }
    ));
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p rakka-agent-mcp --test binding validate_endpoint`
Expected: FAIL to compile — `no method named validate_endpoint found for struct McpServerBinding`.

- [ ] **Step 3: Implement** — in `binding.rs`, replace the URL arm and the protocol-version loop of `validate()` with calls to two new private helpers, and add `validate_endpoint`. `validate()`'s order stays: transport → allow-list → tools → versions.

```rust
    /// Refuses a binding no connection can be made from: an endpoint URL
    /// that fails the URL rule, a child-process binding that names a
    /// server-level credential binding, or a protocol version list that is
    /// empty, malformed, or names a version the pinned SDK does not know.
    ///
    /// The tool allow-list is not read, so a binding with no tools — what a
    /// caller listing a server's tools holds — passes. [`Self::validate`]
    /// makes every one of these checks and the allow-list's too.
    ///
    /// # Errors
    ///
    /// [`McpRegistrationError`] with stable code `mcp-binding-invalid`.
    pub fn validate_endpoint(&self) -> Result<(), McpRegistrationError> {
        match &self.transport {
            McpTransport::StreamableHttp { url } => self.check_url(url)?,
            McpTransport::ChildProcess { .. } => {
                if self.credential_binding.is_some() {
                    return Err(McpRegistrationError::CredentialOnChildProcess {
                        server: self.server_id.to_string(),
                    });
                }
            }
        }
        self.check_protocol_versions()
    }

    /// The endpoint URL rule, as a binding refusal.
    fn check_url(&self, url: &str) -> Result<(), McpRegistrationError> {
        check_streamable_http_url(url).map_err(|reason| McpRegistrationError::InvalidUrl {
            server: self.server_id.to_string(),
            reason,
        })
    }

    /// The protocol version list: non-empty, each well-shaped and known.
    fn check_protocol_versions(&self) -> Result<(), McpRegistrationError> {
        if self.protocol_versions.is_empty() {
            return Err(McpRegistrationError::ProtocolVersionsEmpty {
                server: self.server_id.to_string(),
            });
        }
        for version in &self.protocol_versions {
            if !is_protocol_version_shaped(version) {
                return Err(McpRegistrationError::ProtocolVersionInvalid {
                    server: self.server_id.to_string(),
                    value: version.clone(),
                });
            }
            // Well-shaped is not enough: a version this SDK cannot name is one
            // `connect` could never offer, so it is a misconfigured binding —
            // named here, at the operator's own gate — rather than a sync that
            // reads back as a network failure later.
            if !is_known_protocol_version(version) {
                return Err(McpRegistrationError::ProtocolVersionUnknown {
                    server: self.server_id.to_string(),
                    value: version.clone(),
                });
            }
        }
        Ok(())
    }
```

In `validate()`: the `McpTransport::StreamableHttp { url } => { check_streamable_http_url(url).map_err(..)?; }` arm becomes `McpTransport::StreamableHttp { url } => self.check_url(url)?,`; the trailing `if self.protocol_versions.is_empty() { .. } for version in .. { .. } Ok(())` becomes `self.check_protocol_versions()`. The child-process arm, the allow-list and per-tool checks are untouched. Add to `validate()`'s doc: "[`Self::validate_endpoint`] makes the connection half of these checks alone."

- [ ] **Step 4: Run the binding tests**

Run: `cargo test -p rakka-agent-mcp --test binding`
Expected: PASS, every test (13), including the existing protocol-version and URL tests unchanged.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-mcp/src/binding.rs crates/rakka-agent-mcp/tests/binding.rs
git commit -F - <<'EOF'
Add McpServerBinding::validate_endpoint, the connection half of validate

Issue #85 ask 1 needs a binding with no allow-list checked before a tool
listing dials it. validate_endpoint makes the URL, child-process
credential and protocol-version checks and does not read the tools;
validate keeps every check in its order, through the same helpers.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 2: The server's version on the session and the stored set (ask 4)

**Files:**
- Modify: `crates/rakka-agent-mcp/src/client.rs` (`McpClientSession` ~line 305; `finish` ~line 600)
- Modify: `crates/rakka-agent-mcp/src/testkit.rs` (`FakeState`, `FakeMcpServer::new`, a builder, `get_info`)
- Modify: `crates/rakka-agent-mcp/src/sync.rs` (module doc; `McpDescriptorSet`; `McpDescriptorSetRecord` + `TryFrom`; `CredentialEchoed` doc; `sync_over_session`)
- Modify: `crates/rakka-agent-mcp/src/executor.rs` (unit-test `set()` literal ~line 1234)
- Test: `crates/rakka-agent-mcp/tests/descriptor_sync.rs`, `crates/rakka-agent-mcp/tests/sync_credential_echo.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `pub(crate) fn server_version(&self) -> Option<&str>` on `McpClientSession`.
  - `pub fn with_server_version(self, version: impl Into<String>) -> Self` on `FakeMcpServer`.
  - `pub server_version: Option<String>` on `McpDescriptorSet`.
  - `pub(crate) fn judge_server_identity(server: &str, name: &str, version: Option<&str>, secrets: &[&str]) -> Result<(), McpSyncError>` in `sync.rs` (Task 5 reuses it).
  - `pub(crate) fn bounded_server_name(name: &str) -> String` (already exists; becomes `pub(crate)`).

- [ ] **Step 1: Write the failing tests** — in `tests/descriptor_sync.rs`, add `MCP_SERVER_NAME_MAX_BYTES` to the `rakka_agent_mcp::{..}` import, then append:

```rust
/// `binding`'s two tools, from a server that reports `version`.
async fn synced_with_version(version: &str) -> McpDescriptorSet {
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(tool("search"))
            .with_tool(tool("update"))
            .with_server_version(version),
    )
    .await;
    sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect("syncs")
}

#[tokio::test]
async fn a_synced_set_carries_the_servers_version_cut_on_a_character_boundary() {
    assert_eq!(
        synced_with_version("4.2.0").await.server_version.as_deref(),
        Some("4.2.0")
    );
    // 255 ASCII bytes, then a two-byte character straddling the bound.
    let kept = "v".repeat(MCP_SERVER_NAME_MAX_BYTES - 1);
    let long = format!("{kept}é-and-then-some");
    assert_eq!(
        synced_with_version(&long).await.server_version.as_deref(),
        Some(kept.as_str()),
        "the straddling character is dropped whole"
    );
}

#[tokio::test]
async fn an_empty_server_version_reads_as_none() {
    assert_eq!(synced_with_version("").await.server_version, None);
}

#[tokio::test]
async fn the_server_version_is_display_data_outside_the_digest_and_optional_on_decode() {
    let set = synced_with_version("4.2.0").await;
    let encoded = serde_json::to_value(&set).expect("encodes");
    assert_eq!(encoded["server_version"], json!("4.2.0"));
    assert_eq!(
        serde_json::from_value::<McpDescriptorSet>(encoded.clone()).expect("round-trips"),
        set
    );

    let mut without = encoded;
    without
        .as_object_mut()
        .expect("an object")
        .remove("server_version");
    let decoded = serde_json::from_value::<McpDescriptorSet>(without)
        .expect("a set written before the field decodes");
    assert_eq!(decoded.server_version, None);
    assert_eq!(
        decoded.digest(),
        set.digest(),
        "the version is not part of what a release pins"
    );
    assert!(
        serde_json::to_value(&decoded)
            .expect("encodes")
            .get("server_version")
            .is_none(),
        "an absent version is omitted, as an earlier build wrote the set"
    );
}
```

And in `tests/sync_credential_echo.rs`, append:

```rust
#[tokio::test]
async fn a_server_version_that_carries_the_credential_is_refused() {
    let credential = AgentEphemeralCredential::bearer_token(TOKEN);
    let server = FakeMcpServer::new()
        .with_server_version(format!("1.0-SERVER-WORDS-{TOKEN}"))
        .with_tool(search("Searches.", json!({"type":"object"})));
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "server version",
        None,
        TOKEN,
    );
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test descriptor_sync --test sync_credential_echo`
Expected: FAIL to compile — `no method named with_server_version found for struct FakeMcpServer` and `no field server_version on type McpDescriptorSet`.

- [ ] **Step 3: The fake reports a version** — in `testkit.rs`: add `server_version: String,` to `FakeState` after `server_name`; initialize it `server_version: "fake".to_string(),` in `FakeMcpServer::new`; in `get_info` pass `state.server_version.clone()` where it passes `"fake"` to `rmcp::model::Implementation::new(state.server_name.clone(), ..)`; and add after `with_server_name`:

```rust
    /// Sets the implementation version the server reports, `"fake"` until set.
    #[must_use]
    pub fn with_server_version(self, version: impl Into<String>) -> Self {
        self.lock().server_version = version.into();
        self
    }
```

- [ ] **Step 4: The session keeps it** — in `client.rs`: add `server_version: Option<String>,` to `McpClientSession` after `server_name`; in `finish`, after `server_name` is read:

```rust
    // An empty version is no version: the field is required on the wire, so
    // a server with nothing to say sends "".
    let server_version = peer_info
        .server_info
        .as_ref()
        .map(|info| info.version.clone())
        .filter(|version| !version.is_empty());
```

and construct `McpClientSession { running, server_name, server_version, negotiated }`; add the accessor after `server_name()`:

```rust
    /// The server's self-reported implementation version, `None` when it
    /// reported none or an empty one.
    #[must_use]
    pub(crate) fn server_version(&self) -> Option<&str> {
        self.server_version.as_deref()
    }
```

- [ ] **Step 5: The set stores it** — in `sync.rs`:

After `pub server_name: String,` in `McpDescriptorSet`:

```rust
    /// The server's self-reported implementation version at sync time, cut at
    /// [`MCP_SERVER_NAME_MAX_BYTES`]; `None` when it reported none, or on a
    /// set written before the field existed.
    ///
    /// Display data: not in [`Self::digest`], not checked by
    /// [`Self::validate`], not compared by [`mcp_descriptor_staleness`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_version: Option<String>,
```

In `McpDescriptorSetRecord`, after `server_name: String,` add `#[serde(default)] server_version: Option<String>,` and in the `TryFrom` add `server_version: record.server_version,`.

Make `fn bounded_server_name` `pub(crate)`, and add above `sync_over_session`:

```rust
/// The server's identity text, judged before anything else it said: a server
/// that echoes the credential into its name or its version is refused for
/// that, whatever else its answer got wrong.
pub(crate) fn judge_server_identity(
    server: &str,
    name: &str,
    version: Option<&str>,
    secrets: &[&str],
) -> Result<(), McpSyncError> {
    for (field, text) in [("server name", Some(name)), ("server version", version)] {
        if text.is_some_and(|text| carries(text, secrets)) {
            return Err(McpSyncError::CredentialEchoed {
                server: server.to_string(),
                tool: None,
                field,
            });
        }
    }
    Ok(())
}
```

In `sync_over_session`: after `let reported_name = ..;` add `let reported_version = session.server_version().map(str::to_string);`; replace the `if carries(&reported_name, secrets) { return Err(..) }` block with `judge_server_identity(server, &reported_name, reported_version.as_deref(), secrets)?;` (keep its "Judged first" comment, now "into its name or version"); and in the returned set add `server_version: reported_version.as_deref().map(bounded_server_name),` after `server_name`.

Docs in `sync.rs`: the module doc bullet "The server's self-reported name is cut at [`MCP_SERVER_NAME_MAX_BYTES`]." becomes "The server's self-reported name and version are each cut at [`MCP_SERVER_NAME_MAX_BYTES`]."; the echo bullet "in the server's name, a description, or either schema" becomes "in the server's name or version, a description, or either schema"; `CredentialEchoed`'s `field` doc list becomes "`server name`, `server version`, `description`, `annotations title`, `input schema`, or `output schema`"; its `tool` doc becomes "`None` for the server's name or version".

In `executor.rs`'s unit-test `fn set(..)` literal, add `server_version: None,` after `server_name`.

- [ ] **Step 6: Run the tests**

Run: `cargo test -p rakka-agent-mcp --features testkit --test descriptor_sync --test sync_credential_echo --test descriptor_set_validation --lib`
Expected: PASS. If `a_synced_set_carries_the_servers_version..` reads `None` for `"4.2.0"`, the discovery path is not carrying `server_info`: inspect `peer_info.server_info` in `finish` (systematic debugging), do not loosen the test.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent-mcp/src crates/rakka-agent-mcp/tests/descriptor_sync.rs crates/rakka-agent-mcp/tests/sync_credential_echo.rs
git commit -F - <<'EOF'
Keep the MCP server's self-reported version on the session and the stored set

Issue #85 ask 4. The session keeps server_info.version beside the name
(empty reads as none); McpDescriptorSet gains server_version, optional
and serde-defaulted on schema version 2, cut at the name's bound, outside
the digest, validate and staleness; a version that carries the credential
is refused right after the name. The fake gains with_server_version.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 3: Every missing tool at once (ask 2)

**Files:**
- Modify: `crates/rakka-agent-mcp/src/sync.rs` (`McpSyncError` + `code()` + `Display`; `synced_descriptors` ~line 809)
- Test: `crates/rakka-agent-mcp/tests/descriptor_sync.rs`, `crates/rakka-agent-mcp/tests/sync_credential_echo.rs`

**Interfaces:**
- Consumes: Task 2's `judge_server_identity` ordering (identity echo before the listing's own refusals).
- Produces: `McpSyncError::ToolsNotListed { server: String, tools: Vec<String> }`, `code()` = `mcp-binding-invalid`.

- [ ] **Step 1: Write the failing tests** — in `tests/descriptor_sync.rs`, add `McpSyncError` to the imports if absent, then append:

```rust
/// A binding allow-listing `tools`, each read-only.
fn binding_naming(url: &str, tools: &[&str]) -> McpServerBinding {
    tools.iter().fold(
        McpServerBinding::streamable_http(McpServerId::new("crm").expect("id"), url),
        |binding, tool| {
            binding
                .with_tool(*tool, declared(AgentEffectSafetyClass::ReadOnly))
                .expect("tool")
        },
    )
}

#[tokio::test]
async fn every_allow_listed_tool_the_server_lacks_is_named_at_once_in_tool_name_order() {
    let endpoint = serve_fake(FakeMcpServer::new().with_tool(tool("search"))).await;
    let error = sync_mcp_descriptors(
        &client(),
        &binding_naming(&endpoint.url, &["update", "search", "archive"]),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect_err("two named tools are missing");
    assert_eq!(error.code(), "mcp-binding-invalid");
    assert!(
        matches!(
            &error,
            McpSyncError::ToolsNotListed { server, tools }
                if server == "crm" && tools == &["archive".to_string(), "update".to_string()]
        ),
        "{error:?}"
    );
    assert!(
        error.to_string().contains("archive, update"),
        "the message names both: {error}"
    );
}

#[tokio::test]
async fn a_missing_tool_is_refused_before_a_present_tools_own_refusal() {
    // `search` is over the 64 KiB line, and `update` is missing.
    let endpoint = serve_fake(
        FakeMcpServer::new().with_tool(FakeTool::text("search", "Big.", schema(2_000), "ok")),
    )
    .await;
    let error = sync_mcp_descriptors(
        &client(),
        &binding(&endpoint.url),
        None,
        AgentTimestampMillis::new(1),
        &McpAllowAllEgress,
    )
    .await
    .expect_err("refused");
    assert!(
        matches!(&error, McpSyncError::ToolsNotListed { tools, .. } if tools == &["update".to_string()]),
        "the binding's mismatch first, not the present tool's size: {error:?}"
    );
}
```

And in `tests/sync_credential_echo.rs`, append:

```rust
#[tokio::test]
async fn an_echoed_server_name_is_refused_before_a_missing_tool() {
    let credential = AgentEphemeralCredential::bearer_token(TOKEN);
    // The server lists nothing the binding names, and echoes the credential.
    let server = FakeMcpServer::new().with_server_name(format!("crm-{TOKEN}"));
    assert_echo_refused(
        sync(server, Some(&credential)).await,
        "server name",
        None,
        TOKEN,
    );
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test descriptor_sync --test sync_credential_echo`
Expected: FAIL to compile — `no variant named ToolsNotListed found for enum McpSyncError`. (`an_echoed_server_name_is_refused_before_a_missing_tool` would already pass on its own; it pins the order the refactor must keep.)

- [ ] **Step 3: Implement** — in `sync.rs`, add the variant after `Descriptor { .. }`:

```rust
    /// The server does not list tools the binding's allow-list names.
    ToolsNotListed {
        /// The server listed.
        server: String,
        /// Every allow-listed tool the listing lacks, in tool-name order.
        tools: Vec<String>,
    },
```

`code()`: `Self::Descriptor { .. } | Self::ToolsNotListed { .. } => "mcp-binding-invalid",`. `Display`:

```rust
            Self::ToolsNotListed { server, tools } => write!(
                f,
                "the MCP server {server} does not list the tools {}",
                tools.join(", ")
            ),
```

Replace `synced_descriptors`'s body:

```rust
    // Every allow-listed name is matched first, so a binding that names tools
    // the server lacks hears about all of them at once rather than one per
    // attempt; only then is each present tool judged, in tool-name order.
    let mut found = Vec::with_capacity(binding.tools.len());
    let mut missing = Vec::new();
    for (tool, policy) in &binding.tools {
        match listed.iter().find(|candidate| candidate.name == tool.as_str()) {
            Some(listed) => found.push((tool, policy, listed)),
            None => missing.push(tool.clone()),
        }
    }
    if !missing.is_empty() {
        return Err(McpSyncError::ToolsNotListed {
            server: server.to_string(),
            tools: missing,
        });
    }
    found
        .into_iter()
        .map(|(tool, policy, listed)| synced_descriptor(binding, server, tool, policy, listed, secrets))
        .collect()
```

Update `synced_descriptors`'s doc with one line: "A binding naming tools the server lacks is refused [`McpSyncError::ToolsNotListed`] with every one of them."

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent-mcp --features testkit --test descriptor_sync --test sync_credential_echo --test client_dispatch`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-mcp/src/sync.rs crates/rakka-agent-mcp/tests/descriptor_sync.rs crates/rakka-agent-mcp/tests/sync_credential_echo.rs
git commit -F - <<'EOF'
Name every allow-listed tool the MCP server lacks in one refusal

Issue #85 ask 2. The sync stopped at the first allow-listed name the
listing lacked; it now matches every name first and refuses
McpSyncError::ToolsNotListed with all of them, in tool-name order, under
the same mcp-binding-invalid code, before judging any present tool. An
echoed credential in the server's identity still wins.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 4: `list_mcp_tools` — the listing and its marks (ask 1)

**Files:**
- Create: `crates/rakka-agent-mcp/src/listing.rs`
- Modify: `crates/rakka-agent-mcp/src/sync.rs` (extract and expose helpers)
- Modify: `crates/rakka-agent-mcp/src/lib.rs` (module, map, exports)
- Test: create `crates/rakka-agent-mcp/tests/tool_listing.rs`

**Interfaces:**
- Consumes: Task 1 `McpServerBinding::validate_endpoint`; Task 2 `McpClientSession::server_version`, `bounded_server_name`.
- Produces (public, re-exported at the crate root): `list_mcp_tools`, `list_mcp_tools_within` (signatures in Step 4), `McpToolListing`, `McpListedTool`, `McpListedToolRefusal` with `pub const fn code(&self) -> &'static str`. Crate-private in `sync.rs`: `cut_at(text: &str, max: usize) -> String`, `bounded_description(&str) -> String`, `stored_annotations(&ToolAnnotations) -> McpToolAnnotations`, `pages_unended(server: &str) -> McpSyncError`, `sync_deadline(Duration) -> tokio::time::Instant`, `timed_out(&str, Duration) -> McpSyncError`.

- [ ] **Step 1: Write the failing tests** — create `crates/rakka-agent-mcp/tests/tool_listing.rs`:

```rust
//! `list_mcp_tools`: every tool a server lists, vetted but not bound — under
//! the sync's own rules, with no allow-list, storing nothing.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use std::time::{Duration, Instant};

use rakka_agent::{
    AgentAuthorityRefusal, AgentContentDigest, AgentEffectSafetyClass, AgentToolDeclaration,
};
use rakka_agent_mcp::testkit::{
    hardened_reqwest_client, serve_fake, CountingClient, FakeMcpServer, FakeTool,
};
use rakka_agent_mcp::{
    list_mcp_tools, list_mcp_tools_within, McpAllowAllEgress, McpEgressCheck,
    McpListedToolRefusal, McpServerBinding, McpServerId, McpToolListing, McpToolPolicy,
    MCP_DESCRIPTOR_SCHEMA_MAX_BYTES, MCP_SESSION_CLOSE_BOUND_MS,
};
use rakka_agent_workflow::AgentEphemeralCredential;
use serde_json::{json, Value};

mod support;

fn server_id() -> McpServerId {
    McpServerId::new("crm").expect("id")
}

fn tool_less(url: &str) -> McpServerBinding {
    McpServerBinding::streamable_http(server_id(), url)
}

fn object() -> Value {
    json!({ "type": "object" })
}

/// A schema whose encoding is over the 64 KiB line.
fn oversized() -> Value {
    json!({ "type": "object", "description": "x".repeat(MCP_DESCRIPTOR_SCHEMA_MAX_BYTES) })
}

async fn listed(server: FakeMcpServer) -> McpToolListing {
    let endpoint = serve_fake(server).await;
    list_mcp_tools(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        None,
        &McpAllowAllEgress,
    )
    .await
    .expect("lists")
}

#[tokio::test]
async fn a_listing_returns_every_tool_in_name_order_marking_the_ones_a_sync_would_refuse() {
    let long_name = "n".repeat(300);
    let listing = listed(
        FakeMcpServer::new()
            .with_server_version("4.2.0")
            .with_tool(FakeTool::text("search", "Searches.", object(), "ok").with_output_schema(object()))
            .with_tool(FakeTool::text("big_input", "Big.", oversized(), "ok"))
            .with_tool(FakeTool::text("big_output", "Big out.", object(), "ok").with_output_schema(oversized()))
            .with_tool(FakeTool::text(long_name.clone(), "Long.", object(), "ok"))
            .with_tool(FakeTool::text("bell\u{7}", "Rings.", object(), "ok"))
            .with_tool(FakeTool::text("quiet", "x", object(), "ok").without_description()),
    )
    .await;

    assert_eq!(listing.server_id, server_id());
    assert_eq!(listing.server_name, "fake-mcp-server");
    assert_eq!(listing.server_version.as_deref(), Some("4.2.0"));
    assert_eq!(listing.protocol_version, "2026-07-28");
    let names: Vec<&str> = listing.tools.iter().map(|tool| tool.name.as_str()).collect();
    let cut = "n".repeat(256);
    assert_eq!(
        names,
        ["bell\u{7}", "big_input", "big_output", cut.as_str(), "quiet", "search"]
    );
    let tool = |name: &str| {
        listing
            .tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("{name} is listed"))
    };

    let search = tool("search");
    assert_eq!(search.tool_id.as_ref().map(|id| id.as_str()), Some("mcp.crm.search"));
    assert_eq!(search.description.as_deref(), Some("Searches."));
    assert_eq!(search.input_schema, Some(object()));
    assert_eq!(search.schema_digest, Some(AgentContentDigest::sha256_of_json(&object())));
    assert_eq!(search.output_schema, Some(object()));
    assert_eq!(search.output_schema_digest, Some(AgentContentDigest::sha256_of_json(&object())));
    assert_eq!(search.unusable, None);

    for name in ["big_input", "big_output"] {
        let big = tool(name);
        assert!(
            matches!(big.unusable, Some(McpListedToolRefusal::SchemaTooLarge { bytes }) if bytes > MCP_DESCRIPTOR_SCHEMA_MAX_BYTES),
            "{name}: {:?}",
            big.unusable
        );
        assert!(big.tool_id.is_some(), "{name}'s name is bindable");
        assert!(big.description.is_some(), "{name} keeps its description");
        assert_eq!(
            (&big.input_schema, &big.schema_digest, &big.output_schema, &big.output_schema_digest),
            (&None, &None, &None, &None),
            "{name}'s schemas are withheld"
        );
    }
    assert_eq!(
        tool("big_input").unusable.as_ref().map(McpListedToolRefusal::code),
        Some("mcp-descriptor-schema-too-large")
    );

    for name in [cut.as_str(), "bell\u{7}"] {
        let misnamed = tool(name);
        assert!(
            matches!(misnamed.unusable, Some(McpListedToolRefusal::NameInvalid { .. })),
            "{:?}",
            misnamed.unusable
        );
        assert_eq!(misnamed.tool_id, None);
        assert_eq!(misnamed.input_schema, None);
        assert_eq!(
            misnamed.unusable.as_ref().map(McpListedToolRefusal::code),
            Some("mcp-binding-invalid")
        );
    }

    let quiet = tool("quiet");
    assert_eq!(quiet.description, None);
    assert_eq!(quiet.unusable, None);
}

#[tokio::test]
async fn a_name_at_the_tool_id_boundary_is_bindable_and_one_byte_more_is_not() {
    // `mcp.crm.` is 8 bytes; a tool id is at most 256.
    let fits = "a".repeat(248);
    let over = "b".repeat(249);
    let listing = listed(
        FakeMcpServer::new()
            .with_tool(FakeTool::text(fits.clone(), "Fits.", object(), "ok"))
            .with_tool(FakeTool::text(over.clone(), "Over.", object(), "ok")),
    )
    .await;
    assert_eq!(listing.tools[0].name, fits);
    assert!(listing.tools[0].tool_id.is_some());
    assert_eq!(listing.tools[0].unusable, None);
    assert_eq!(listing.tools[1].name, over, "under 256 bytes, returned whole");
    assert!(matches!(
        listing.tools[1].unusable,
        Some(McpListedToolRefusal::NameInvalid { .. })
    ));
}

#[tokio::test]
async fn the_allow_list_is_not_read_so_a_named_tool_the_server_lacks_is_no_refusal() {
    let endpoint = serve_fake(
        FakeMcpServer::new().with_tool(FakeTool::text("search", "Searches.", object(), "ok")),
    )
    .await;
    let named = tool_less(&endpoint.url)
        .with_tool(
            "archive",
            McpToolPolicy::new(AgentToolDeclaration::new(AgentEffectSafetyClass::ReadOnly)),
        )
        .expect("tool");
    let listing = list_mcp_tools(&hardened_reqwest_client(), &named, None, &McpAllowAllEgress)
        .await
        .expect("lists regardless of the allow-list");
    let names: Vec<&str> = listing.tools.iter().map(|tool| tool.name.as_str()).collect();
    assert_eq!(names, ["search"]);
}

#[tokio::test]
async fn a_server_with_no_tools_lists_as_empty() {
    let endpoint = serve_fake(FakeMcpServer::new()).await;
    let listing = list_mcp_tools(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        None,
        &McpAllowAllEgress,
    )
    .await
    .expect("an empty server is a listing, not a refusal");
    assert!(listing.tools.is_empty());
    assert_eq!(endpoint.server.handshakes(), 1);
    assert_eq!(endpoint.server.list_calls(), 1);
}

#[tokio::test]
async fn a_name_the_server_lists_twice_is_one_entry_from_its_first_copy() {
    let listing = listed(
        FakeMcpServer::new()
            .with_tool(FakeTool::text("search", "First.", object(), "ok"))
            .with_tool(FakeTool::text("search", "Second.", object(), "ok")),
    )
    .await;
    assert_eq!(listing.tools.len(), 1);
    assert_eq!(listing.tools[0].description.as_deref(), Some("First."));
}

#[tokio::test]
async fn an_empty_server_version_lists_as_none() {
    let listing = listed(FakeMcpServer::new().with_server_version("")).await;
    assert_eq!(listing.server_version, None);
}

struct RefuseEveryDestination;

impl McpEgressCheck for RefuseEveryDestination {
    fn check(&self, _server: &McpServerId, _url: &str) -> Result<(), AgentAuthorityRefusal> {
        Err(AgentAuthorityRefusal::of("egress-denied", "not on the allowlist"))
    }
}

#[tokio::test]
async fn a_refusing_egress_rule_stops_the_listing_before_any_request() {
    let endpoint = serve_fake(FakeMcpServer::new()).await;
    let counting = CountingClient::new();
    let credential = AgentEphemeralCredential::bearer_token("never-sent-sentinel");
    let error = list_mcp_tools(
        &counting,
        &tool_less(&endpoint.url),
        Some(&credential),
        &RefuseEveryDestination,
    )
    .await
    .expect_err("egress refuses");
    assert_eq!(error.code(), "egress-denied");
    assert_eq!(counting.sends(), 0, "no transport method ran");
    assert_eq!(endpoint.server.handshakes(), 0, "the server saw nothing");
}

#[tokio::test]
async fn the_credential_reaches_the_wire_as_its_header() {
    let endpoint = serve_fake(FakeMcpServer::new()).await;
    let credential = AgentEphemeralCredential::bearer_token("listing-token-sentinel");
    list_mcp_tools(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        Some(&credential),
        &McpAllowAllEgress,
    )
    .await
    .expect("lists");
    let seen = endpoint.server.seen_headers();
    assert!(
        seen.iter().any(|(name, value)| name.eq_ignore_ascii_case("authorization")
            && value.ends_with("listing-token-sentinel")),
        "{seen:?}"
    );
}

#[tokio::test]
async fn a_server_that_identifies_as_a_rakka_agent_is_refused() {
    let endpoint = serve_fake(FakeMcpServer::new().with_server_name("rakka-agent-support")).await;
    let error = list_mcp_tools(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        None,
        &McpAllowAllEgress,
    )
    .await
    .expect_err("peer channel");
    assert_eq!(error.code(), "mcp-peer-agent-channel-refused");
}

#[tokio::test]
async fn a_listing_that_stalls_ends_at_its_bound() {
    let endpoint = serve_fake(FakeMcpServer::new().with_list_delay(5_000)).await;
    let started = Instant::now();
    let error = list_mcp_tools_within(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        None,
        &McpAllowAllEgress,
        Duration::from_millis(300),
    )
    .await
    .expect_err("the listing never answered inside the bound");
    assert_eq!(error.code(), "mcp-descriptor-sync-failed", "{error}");
    assert!(
        started.elapsed() < Duration::from_millis(300 + MCP_SESSION_CLOSE_BOUND_MS + 1_700),
        "the bound, plus the session's own bounded close: {:?}",
        started.elapsed()
    );
    assert_eq!(endpoint.server.list_calls(), 1);
}

#[tokio::test]
async fn a_listing_whose_pages_never_end_is_refused() {
    let endpoint = serve_fake(
        FakeMcpServer::new()
            .with_tool(FakeTool::text("search", "Searches.", object(), "ok"))
            .with_endless_pages(),
    )
    .await;
    let error = list_mcp_tools(
        &hardened_reqwest_client(),
        &tool_less(&endpoint.url),
        None,
        &McpAllowAllEgress,
    )
    .await
    .expect_err("endless pages");
    assert_eq!(error.code(), "mcp-descriptor-sync-failed", "{error}");
}

#[tokio::test]
async fn a_child_process_binding_is_refused_as_no_http_endpoint() {
    let child = McpServerBinding::child_process(server_id(), support::spec_artifact_ref());
    let counting = CountingClient::new();
    let error = list_mcp_tools(&counting, &child, None, &McpAllowAllEgress)
        .await
        .expect_err("the HTTP listing has no endpoint to dial");
    assert_eq!(error.code(), "mcp-descriptor-sync-failed", "{error}");
    assert_eq!(counting.sends(), 0);
}

#[tokio::test]
async fn a_binding_with_an_invalid_endpoint_is_refused_before_egress() {
    let counting = CountingClient::new();
    let error = list_mcp_tools(
        &counting,
        &tool_less("ftp://h/mcp"),
        None,
        &RefuseEveryDestination,
    )
    .await
    .expect_err("the URL rule");
    assert_eq!(error.code(), "mcp-binding-invalid", "the binding, not the egress rule: {error}");
    assert_eq!(counting.sends(), 0);
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test tool_listing`
Expected: FAIL to compile — `unresolved imports rakka_agent_mcp::list_mcp_tools, ..::McpToolListing, ..::McpListedToolRefusal`.

- [ ] **Step 3: Expose the sync's helpers** — in `sync.rs`, make `sync_deadline` and `timed_out` `pub(crate)`, and replace the two cutting functions with one shared cut:

```rust
/// `text` cut at `max` bytes, on a character boundary.
pub(crate) fn cut_at(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

/// The server's name or version as a set or a listing keeps it: cut at
/// [`MCP_SERVER_NAME_MAX_BYTES`], on a character boundary.
pub(crate) fn bounded_server_name(name: &str) -> String {
    cut_at(name, MCP_SERVER_NAME_MAX_BYTES)
}

/// Truncates a server-supplied description to the descriptor bound, at a
/// character boundary: the server's text is untrusted input, and a descriptor
/// that exceeds the bound would be refused wholesale instead.
pub(crate) fn bounded_description(description: &str) -> String {
    cut_at(description, AGENT_TOOL_DESCRIPTION_MAX_LENGTH)
}

/// The server's hints as this adapter keeps them, the title cut as a
/// description is.
pub(crate) fn stored_annotations(annotations: &ToolAnnotations) -> McpToolAnnotations {
    McpToolAnnotations {
        title: annotations.title.as_deref().map(bounded_description),
        read_only_hint: annotations.read_only_hint,
        destructive_hint: annotations.destructive_hint,
        idempotent_hint: annotations.idempotent_hint,
        open_world_hint: annotations.open_world_hint,
    }
}

/// What a `tools/list` that had not ended after [`MCP_LIST_PAGES_MAX`] pages
/// answers.
pub(crate) fn pages_unended(server: &str) -> McpSyncError {
    McpSyncError::Client(McpClientError::Protocol {
        server: server.to_string(),
        reason: format!("tools/list did not end within {MCP_LIST_PAGES_MAX} pages"),
    })
}
```

In `synced_descriptor`, replace the inline `McpToolAnnotations { .. }` construction with `listed.annotations.as_ref().map(stored_annotations)`; in `sync_over_session`, replace the inline `McpSyncError::Client(McpClientError::Protocol { .. "did not end within" .. })` with `pages_unended(server)`.

Run: `cargo test -p rakka-agent-mcp --features testkit --test descriptor_sync --test sync_credential_echo --lib`
Expected: PASS (behaviour-preserving refactor).

- [ ] **Step 4: Write the listing** — create `crates/rakka-agent-mcp/src/listing.rs`:

```rust
//! `list_mcp_tools`: every tool one MCP server lists, vetted but not bound.
//!
//! A listing is what a caller shows a person choosing tools, before any
//! binding names them. It runs the sync's own path — the binding's endpoint
//! validated, the egress check before a client exists, the credential read
//! below it, the peer-agent rule, one `tools/list` of at most
//! [`MCP_LIST_PAGES_MAX`](crate::MCP_LIST_PAGES_MAX) pages under one deadline,
//! the session closed on every path — with no allow-list. It stores nothing
//! and mints no binding: the sync stays the only producer of release data.
//!
//! A tool a sync would refuse for its own sake — a schema over
//! [`MCP_DESCRIPTOR_SCHEMA_MAX_BYTES`], or a name that cannot form a tool id —
//! is listed and marked [`McpListedTool::unusable`], its schemas withheld, so
//! one such tool never hides the rest.

use std::collections::BTreeMap;
use std::time::Duration;

use rakka_agent::{AgentContentDigest, AgentToolId, AGENT_IDENTITY_MAX_LENGTH};
use rakka_agent_workflow::AgentEphemeralCredential;
use rmcp::model::Tool;
use rmcp::transport::streamable_http_client::StreamableHttpClient;
use serde_json::Value;

use crate::binding::{
    McpRegistrationError, McpServerBinding, McpServerId, MCP_DESCRIPTOR_SCHEMA_MAX_BYTES,
    MCP_SYNC_TIMEOUT_DEFAULT_MS,
};
use crate::client::{connect, service_error, McpEgressCheck};
use crate::sync::{
    bounded_description, bounded_server_name, cut_at, pages_unended, stored_annotations,
    sync_deadline, timed_out, McpSyncError, McpToolAnnotations,
};

/// What one MCP server lists, vetted but not bound.
#[derive(Debug, Clone, PartialEq)]
pub struct McpToolListing {
    /// The server listed.
    pub server_id: McpServerId,
    /// The server's self-reported name, cut at
    /// [`MCP_SERVER_NAME_MAX_BYTES`](crate::MCP_SERVER_NAME_MAX_BYTES).
    pub server_name: String,
    /// The server's self-reported version, cut the same way; `None` when it
    /// reported none or an empty one.
    pub server_version: Option<String>,
    /// The protocol version the listing negotiated.
    pub protocol_version: String,
    /// One entry per tool name, in tool-name order.
    pub tools: Vec<McpListedTool>,
}

/// One tool as the server lists it.
#[derive(Debug, Clone, PartialEq)]
pub struct McpListedTool {
    /// The server's own name for the tool, cut at
    /// [`AGENT_IDENTITY_MAX_LENGTH`] when it cannot form a tool id.
    pub name: String,
    /// `mcp.<server>.<name>`, when that forms a valid tool id.
    pub tool_id: Option<AgentToolId>,
    /// The server's description, cut at the descriptor bound on a character
    /// boundary; `None` when it gave none or an empty one.
    pub description: Option<String>,
    /// The tool's hints, the title cut as the description is. Reported,
    /// never judged: a listing has no operator declaration to contradict.
    pub annotations: Option<McpToolAnnotations>,
    /// The raw input schema; `None` when the tool is unusable.
    pub input_schema: Option<Value>,
    /// SHA-256 of `input_schema`, as a sync would pin it.
    pub schema_digest: Option<AgentContentDigest>,
    /// The raw output schema, when declared; `None` when the tool is unusable.
    pub output_schema: Option<Value>,
    /// SHA-256 of `output_schema`.
    pub output_schema_digest: Option<AgentContentDigest>,
    /// Why a sync naming this tool would be refused, when it would be.
    pub unusable: Option<McpListedToolRefusal>,
}

/// Why a listed tool cannot be bound.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum McpListedToolRefusal {
    /// An input or output schema is over [`MCP_DESCRIPTOR_SCHEMA_MAX_BYTES`].
    SchemaTooLarge {
        /// The larger offending schema's encoded size.
        bytes: usize,
    },
    /// The name cannot form a valid tool id.
    NameInvalid {
        /// The tool-id rule's own reason.
        reason: String,
    },
}

impl McpListedToolRefusal {
    /// The stable code a sync naming this tool would be refused under.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::SchemaTooLarge { .. } => "mcp-descriptor-schema-too-large",
            Self::NameInvalid { .. } => "mcp-binding-invalid",
        }
    }
}

/// Lists every tool one MCP server exposes, under the sync's own rules and
/// [`MCP_SYNC_TIMEOUT_DEFAULT_MS`].
///
/// The binding's allow-list is not read; [`McpServerBinding::validate_endpoint`]
/// is the only check of the binding. `egress` is required, as for a sync.
///
/// # Errors
///
/// [`McpSyncError`] with its stable code; a listing that ran out of time is
/// `mcp-descriptor-sync-failed`.
pub async fn list_mcp_tools<C>(
    http: &C,
    binding: &McpServerBinding,
    credential: Option<&AgentEphemeralCredential>,
    egress: &dyn McpEgressCheck,
) -> Result<McpToolListing, McpSyncError>
where
    C: StreamableHttpClient + Sync,
{
    list_mcp_tools_within(
        http,
        binding,
        credential,
        egress,
        Duration::from_millis(MCP_SYNC_TIMEOUT_DEFAULT_MS),
    )
    .await
}

/// As [`list_mcp_tools`], under the caller's own bound.
///
/// `timeout` covers the handshake and the listing together; the call returns
/// within it plus
/// [`MCP_SESSION_CLOSE_BOUND_MS`](crate::MCP_SESSION_CLOSE_BOUND_MS).
///
/// # Errors
///
/// [`McpSyncError`] with its stable code; a listing that ran out of time is
/// `mcp-descriptor-sync-failed`.
pub async fn list_mcp_tools_within<C>(
    http: &C,
    binding: &McpServerBinding,
    credential: Option<&AgentEphemeralCredential>,
    egress: &dyn McpEgressCheck,
    timeout: Duration,
) -> Result<McpToolListing, McpSyncError>
where
    C: StreamableHttpClient + Sync,
{
    binding.validate_endpoint()?;
    let server = binding.server_id.to_string();
    let deadline = sync_deadline(timeout);
    let session = tokio::time::timeout_at(deadline, connect(http, binding, credential, egress))
        .await
        .map_err(|_| timed_out(&server, timeout))??;
    let listed = tokio::time::timeout_at(deadline, session.list_all_tools()).await;
    let protocol_version = session.negotiated_version().as_str().to_string();
    let reported_name = session.server_name().to_string();
    let reported_version = session.server_version().map(str::to_string);
    // Closed before the answer is judged, as the sync closes its session.
    session.close().await;
    let listed = listed
        .map_err(|_| timed_out(&server, timeout))?
        .map_err(|error| McpSyncError::Client(service_error(&server, &error)))?
        .ok_or_else(|| pages_unended(&server))?;
    Ok(McpToolListing {
        server_id: binding.server_id.clone(),
        server_name: bounded_server_name(&reported_name),
        server_version: reported_version.as_deref().map(bounded_server_name),
        protocol_version,
        tools: listed_tools(binding, &listed),
    })
}

/// One entry per name, from its first copy — as a sync matches an
/// allow-listed name to its first copy — in name order.
fn listed_tools(binding: &McpServerBinding, listed: &[Tool]) -> Vec<McpListedTool> {
    let mut first: BTreeMap<&str, &Tool> = BTreeMap::new();
    for tool in listed {
        first.entry(tool.name.as_ref()).or_insert(tool);
    }
    first
        .into_values()
        .map(|tool| listed_tool(binding, tool))
        .collect()
}

/// One tool, marked when a sync naming it would be refused for its own sake.
fn listed_tool(binding: &McpServerBinding, listed: &Tool) -> McpListedTool {
    let name: &str = listed.name.as_ref();
    let description = listed
        .description
        .as_deref()
        .filter(|description| !description.is_empty())
        .map(bounded_description);
    let annotations = listed.annotations.as_ref().map(stored_annotations);
    // The name first: a tool that cannot be bound makes its schemas moot.
    let tool_id = match binding.tool_id(name) {
        Ok(tool_id) => tool_id,
        Err(refusal) => {
            let reason = match refusal {
                McpRegistrationError::ToolNameInvalid { reason, .. } => reason,
                other => other.to_string(),
            };
            return unusable(
                cut_at(name, AGENT_IDENTITY_MAX_LENGTH),
                None,
                description,
                annotations,
                McpListedToolRefusal::NameInvalid { reason },
            );
        }
    };
    let input_schema = Value::Object((*listed.input_schema).clone());
    let output_schema = listed
        .output_schema
        .as_ref()
        .map(|schema| Value::Object((**schema).clone()));
    let largest = encoded_size(&input_schema).max(output_schema.as_ref().map_or(0, encoded_size));
    if largest > MCP_DESCRIPTOR_SCHEMA_MAX_BYTES {
        return unusable(
            name.to_string(),
            Some(tool_id),
            description,
            annotations,
            McpListedToolRefusal::SchemaTooLarge { bytes: largest },
        );
    }
    McpListedTool {
        name: name.to_string(),
        tool_id: Some(tool_id),
        description,
        annotations,
        schema_digest: Some(AgentContentDigest::sha256_of_json(&input_schema)),
        input_schema: Some(input_schema),
        output_schema_digest: output_schema
            .as_ref()
            .map(AgentContentDigest::sha256_of_json),
        output_schema,
        unusable: None,
    }
}

/// A tool listed with its schemas withheld.
fn unusable(
    name: String,
    tool_id: Option<AgentToolId>,
    description: Option<String>,
    annotations: Option<McpToolAnnotations>,
    refusal: McpListedToolRefusal,
) -> McpListedTool {
    McpListedTool {
        name,
        tool_id,
        description,
        annotations,
        input_schema: None,
        schema_digest: None,
        output_schema: None,
        output_schema_digest: None,
        unusable: Some(refusal),
    }
}

/// A schema's encoded size; one that cannot be re-encoded counts as over
/// every bound, failing closed.
fn encoded_size(schema: &Value) -> usize {
    serde_json::to_vec(schema).map_or(usize::MAX, |encoded| encoded.len())
}
```

In `lib.rs`: add `pub mod listing;` after `pub mod launcher;`; add to the module map after the `sync` line: "- `listing`: `list_mcp_tools`, every tool a server lists under the sync's rules, marked where a sync would refuse it, stored nowhere."; and add the export `pub use listing::{list_mcp_tools, list_mcp_tools_within, McpListedTool, McpListedToolRefusal, McpToolListing};`.

- [ ] **Step 5: Run the listing tests**

Run: `cargo test -p rakka-agent-mcp --features testkit --test tool_listing`
Expected: PASS, 13 tests. A failure reading the long name or control-character name off the wire is the fake's or rmcp's, not the listing's: check what `listed.name` held (systematic debugging) before changing either test.

- [ ] **Step 6: Run the crate**

Run: `cargo test -p rakka-agent-mcp --all-features`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/rakka-agent-mcp/src crates/rakka-agent-mcp/tests/tool_listing.rs
git commit -F - <<'EOF'
List every tool an MCP server exposes, marking the ones a sync would refuse

Issue #85 ask 1. list_mcp_tools (and _within) runs the sync's own path —
validate_endpoint, the egress check before a client, the credential below
it, the peer-agent rule, one bounded tools/list, the session closed — with
no allow-list, and answers McpToolListing: one entry per name in name
order, from its first copy. A tool over the schema bound or whose name
cannot form a tool id is marked unusable with its schemas withheld rather
than refusing the listing. Nothing is stored and no binding is minted.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 5: An echoed credential refuses the whole listing

**Files:**
- Modify: `crates/rakka-agent-mcp/src/listing.rs`
- Modify: `crates/rakka-agent-mcp/src/sync.rs` (`carries`, `value_carries` become `pub(crate)`; `CredentialEchoed` field doc gains `tool name`)
- Test: create `crates/rakka-agent-mcp/tests/listing_credential_echo.rs`

**Interfaces:**
- Consumes: Task 2 `judge_server_identity`; Task 4 `list_mcp_tools`.
- Produces: no new public names; the listing refuses `McpSyncError::CredentialEchoed` with `field` one of `server name`, `server version`, `tool name` (with `tool: None`), `description`, `annotations title`, `input schema`, `output schema`.

- [ ] **Step 1: Write the failing tests** — create `crates/rakka-agent-mcp/tests/listing_credential_echo.rs`:

```rust
//! A tool listing against a server that echoes the credential it was sent:
//! every piece of server text the listing received is judged, and any one
//! that carries it refuses the whole listing.
//!
//! The whole file is gated: it drives the in-process fake server, which only
//! exists under `testkit`.
#![cfg(feature = "testkit")]

use rakka_agent_mcp::testkit::{hardened_reqwest_client, serve_fake, FakeMcpServer, FakeTool};
use rakka_agent_mcp::{
    list_mcp_tools, McpAllowAllEgress, McpServerBinding, McpServerId, McpSyncError,
    McpToolListing, MCP_DESCRIPTOR_SCHEMA_MAX_BYTES,
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
    assert!(!text.contains(TOKEN), "the refusal leaks the secret: {text}");
    assert!(!text.contains("SERVER-WORDS"), "the refusal quotes the server: {text}");
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
    assert_echo_refused(list(server, Some(&bearer())).await, "description", Some("search"));
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
    assert_echo_refused(list(server, Some(&bearer())).await, "input schema", Some("search"));
}

#[tokio::test]
async fn an_output_schema_that_carries_the_credential_is_refused() {
    let server = FakeMcpServer::new().with_tool(search().with_output_schema(json!({
        "type": "object",
        "description": format!("SERVER-WORDS {TOKEN}")
    })));
    assert_echo_refused(list(server, Some(&bearer())).await, "output schema", Some("search"));
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
    assert_echo_refused(list(server, Some(&bearer())).await, "input schema", Some("big"));
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
    assert_echo_refused(list(server, Some(&bearer())).await, "description", Some("search"));
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
    list(server(), None).await.expect("lists with no credential");
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p rakka-agent-mcp --features testkit --test listing_credential_echo`
Expected: FAIL — every `assert_echo_refused` test panics with `an echoed credential refuses the listing: Ok(McpToolListing { .. })`; `the_same_text_lists_when_it_is_not_the_credential` passes.

- [ ] **Step 3: Implement** — in `sync.rs`, make `carries` and `value_carries` `pub(crate)`, and in `CredentialEchoed`'s `field` doc list add `tool name` ("`server name`, `server version`, `tool name`, `description`, …"), and its `tool` doc: "`None` for the server's name or version, and for a tool name, which the refusal cannot quote".

In `listing.rs`: extend the imports — `use crate::client::{connect, credential_secrets, service_error, McpEgressCheck};`, and add `carries, judge_server_identity, value_carries` to the `crate::sync` import; add `use serde_json::{Map, Value};`. In `list_mcp_tools_within`, after `let session = ..??;` add `let secrets = credential_secrets(credential);`, and replace

```rust
    let listed = listed
        .map_err(|_| timed_out(&server, timeout))?
```

with

```rust
    // Judged first, as the sync judges it: a server that echoes the
    // credential into its identity is refused for that, whatever else its
    // answer got wrong.
    judge_server_identity(&server, &reported_name, reported_version.as_deref(), &secrets)?;
    let listed = listed
        .map_err(|_| timed_out(&server, timeout))?
```

and, after the `.ok_or_else(|| pages_unended(&server))?;` statement:

```rust
    // Every tool the server listed, every copy, before anything is built.
    for tool in &listed {
        judge_listed_tool(&server, tool, &secrets)?;
    }
```

Add:

```rust
/// Every piece of text the server listed for one tool, judged raw and whole —
/// including a schema the listing would withhold and a duplicate's later
/// copy. "Every byte the listing received" is the rule with no exceptions to
/// reason about: a server that reflects the credential into any of its text
/// is refused for that, whatever that text was going to be shown as.
fn judge_listed_tool(server: &str, listed: &Tool, secrets: &[&str]) -> Result<(), McpSyncError> {
    let name: &str = listed.name.as_ref();
    if carries(name, secrets) {
        // The name is the echo, so the refusal cannot quote it.
        return Err(McpSyncError::CredentialEchoed {
            server: server.to_string(),
            tool: None,
            field: "tool name",
        });
    }
    let echoed = |field: &'static str| McpSyncError::CredentialEchoed {
        server: server.to_string(),
        tool: Some(cut_at(name, AGENT_IDENTITY_MAX_LENGTH)),
        field,
    };
    if listed
        .description
        .as_deref()
        .is_some_and(|description| carries(description, secrets))
    {
        return Err(echoed("description"));
    }
    if listed
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.title.as_deref())
        .is_some_and(|title| carries(title, secrets))
    {
        return Err(echoed("annotations title"));
    }
    if object_carries(&listed.input_schema, secrets) {
        return Err(echoed("input schema"));
    }
    if listed
        .output_schema
        .as_ref()
        .is_some_and(|schema| object_carries(schema, secrets))
    {
        return Err(echoed("output schema"));
    }
    Ok(())
}

/// [`value_carries`] over a schema object, without copying it.
fn object_carries(object: &Map<String, Value>, secrets: &[&str]) -> bool {
    object
        .iter()
        .any(|(key, nested)| carries(key, secrets) || value_carries(nested, secrets))
}
```

Add to the module doc's second paragraph: "Every piece of server text the listing received — the server's name and version, and each listed tool's name, description, title and schemas, withheld or not — is judged first, and one that carries the credential the call sent refuses the whole listing (`mcp-descriptor-credential-echoed`)."

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent-mcp --features testkit --test listing_credential_echo --test tool_listing --test sync_credential_echo`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-mcp/src crates/rakka-agent-mcp/tests/listing_credential_echo.rs
git commit -F - <<'EOF'
Refuse a whole MCP tool listing whose server text echoes the credential

Every piece of server text the listing received is judged before it is
built — the server's name and version, then each listed tool's name,
description, annotation title and schemas, duplicates and withheld
schemas included — and one that carries the credential the call sent
refuses the listing as mcp-descriptor-credential-echoed, naming the
field. A tool-name echo names no tool, since the name is the echo.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 6: rmcp-free annotations on the fake

**Files:**
- Modify: `crates/rakka-agent-mcp/src/testkit.rs` (`FakeTool::with_annotations` ~line 160; a `From` impl)
- Test: `crates/rakka-agent-mcp/tests/fake_server.rs`

**Interfaces:**
- Consumes: `McpToolAnnotations` (exists).
- Produces: `pub fn with_annotations(self, annotations: impl Into<ToolAnnotations>) -> Self`; `impl From<McpToolAnnotations> for ToolAnnotations` (under `testkit`).

- [ ] **Step 1: Write the failing test** — in `tests/fake_server.rs`, add `McpToolAnnotations` to the `rakka_agent_mcp::{..}` import and append:

```rust
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
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p rakka-agent-mcp --features testkit --test fake_server annotations_set_without_rmcp`
Expected: FAIL to compile — `mismatched types: expected ToolAnnotations, found McpToolAnnotations`.

- [ ] **Step 3: Implement** — in `testkit.rs`, add `McpToolAnnotations` to the crate import (`use crate::McpToolAnnotations;` or the existing `crate::sync` import), replace `with_annotations`:

```rust
    /// Declares the tool's hints — rmcp's own `ToolAnnotations`, or this
    /// crate's [`McpToolAnnotations`], so a deployment's proofs need no rmcp
    /// dependency of their own.
    #[must_use]
    pub fn with_annotations(mut self, annotations: impl Into<ToolAnnotations>) -> Self {
        self.annotations = Some(annotations.into());
        self
    }
```

and add after `impl FakeTool`:

```rust
/// The stored hints as the fake lists them. rmcp's type is
/// `#[non_exhaustive]`, so it is built through its own `from_raw`.
impl From<McpToolAnnotations> for ToolAnnotations {
    fn from(annotations: McpToolAnnotations) -> Self {
        ToolAnnotations::from_raw(
            annotations.title,
            annotations.read_only_hint,
            annotations.destructive_hint,
            annotations.idempotent_hint,
            annotations.open_world_hint,
        )
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p rakka-agent-mcp --features testkit --test fake_server --test descriptor_sync --test sync_credential_echo --test listing_credential_echo`
Expected: PASS — the new test, and every existing caller that passes rmcp's `ToolAnnotations`.

- [ ] **Step 5: Commit**

```bash
git add crates/rakka-agent-mcp/src/testkit.rs crates/rakka-agent-mcp/tests/fake_server.rs
git commit -F - <<'EOF'
Let the MCP test fake take McpToolAnnotations, so proofs need no rmcp

FakeTool::with_annotations takes impl Into<ToolAnnotations>, and the
test kit converts McpToolAnnotations through rmcp's from_raw, so a
deployment sets a fake tool's hints with this crate's own type; callers
passing rmcp's type compile unchanged.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 7: Docs and validation

**Files:**
- Modify: `crates/rakka-agent-mcp/src/client.rs` (`McpClientSession` doc ~line 300)
- Modify: `docs/rakka-compatibility.md` (a new bullet after the slice 7.10 bullet, line 94)
- Modify: `CHANGELOG.md` (`### Fixed`, after the batch-1 entry ending "(issue #85, asks 5, 8, 9, 11, and 13).")
- Modify: `docs/rakka-agents.md` (the `rakka-agent-mcp` crate row, line 42)
- Modify: `docs/superpowers/specs/2026-10-11-mcp-discovery-design.md` (status line)

**Interfaces:** none.

- [ ] **Step 1: Session doc** — in `client.rs`, the `McpClientSession` doc sentence "the only public ways to reach a server are the descriptor syncs and the dispatch executor, each of which holds the rules a bare session would not" becomes "the only public ways to reach a server are the descriptor syncs, the tool listing, and the dispatch executor, each of which holds the rules a bare session would not".

- [ ] **Step 2: Compatibility bullet** — insert after the `- Phase 7 slice 7.10 ("follow-ups and phase close") …` line of `docs/rakka-compatibility.md`, as one line:

```markdown
- Issue #85 asks 1, 2, and 4 ("MCP discovery") add a public tool listing, one `McpSyncError` variant, one optional field on the stored descriptor set, and one binding method; no code, no schema version, and no digest changes. **The listing:** `list_mcp_tools` (bounded by `MCP_SYNC_TIMEOUT_DEFAULT_MS`) and `list_mcp_tools_within` answer `McpToolListing` — `server_id`, `server_name`, `server_version`, `protocol_version`, and one `McpListedTool` per tool name in name order, from its first copy — under the sync's own path: the binding checked by the new `McpServerBinding::validate_endpoint()` (the URL, child-process-credential, and protocol-version checks of `validate()`, without reading the allow-list, which the listing ignores), the egress check before a client exists, the credential read below it, the peer-agent rule, one `tools/list` of at most `MCP_LIST_PAGES_MAX` pages, and the session closed on every path; errors are `McpSyncError` under its existing codes, a child-process binding is refused `mcp-descriptor-sync-failed`, and nothing is stored or bound. A listed tool whose input or output schema is over `MCP_DESCRIPTOR_SCHEMA_MAX_BYTES`, or whose name cannot form a tool id, is listed with `unusable` set (`McpListedToolRefusal::SchemaTooLarge { bytes }`, code `mcp-descriptor-schema-too-large`, or `NameInvalid { reason }`, code `mcp-binding-invalid`, which wins when both apply) and its schemas and digests withheld; such a name is cut at 256 bytes. Every piece of server text the listing received — the server's name and version, and each listed tool's name, description, annotation title, and schemas, withheld ones and later copies of a repeated name included — that carries the credential the call sent refuses the whole listing `mcp-descriptor-credential-echoed`, with the new fields `server version` and `tool name` (the latter naming no tool). **Every missing name:** a sync whose allow-list names tools the server does not list is refused `McpSyncError::ToolsNotListed { server, tools }` with all of them in tool-name order, under the `mcp-binding-invalid` the single-tool refusal had, after an echo in the server's name or version and before any present tool's own refusal; the single-tool `Descriptor { reason: "the server does not list it" }` no longer occurs. **The server's version:** the client keeps `server_info.version` (empty reads as none), and `McpDescriptorSet` gains `server_version: Option<String>` on schema version 2 — serde-defaulted and omitted when `None`, cut at `MCP_SERVER_NAME_MAX_BYTES`, outside `digest()`, `validate()`, and `mcp_descriptor_staleness` — so a build from `317463d` on decodes a set that carries it and drops the field; a sync whose server version carries the credential is refused right after the name. **Source-breaking, compile-time only:** `McpDescriptorSet` gains a public field, so a struct literal of it (rakka.host's catalog test kit has one) adds `server_version: None`. The test kit's `FakeMcpServer` gains `with_server_version`, and `FakeTool::with_annotations` takes `impl Into<ToolAnnotations>`, which `McpToolAnnotations` now converts into, so a proof sets a fake tool's hints with no rmcp dependency.
```

- [ ] **Step 3: Changelog** — append to `### Fixed`, after the batch-1 entry:

```markdown
- A deployment could not list an MCP server's tools without naming them first, heard about one missing allow-listed tool per sync, and could show no server version. `list_mcp_tools` and `list_mcp_tools_within` now answer an `McpToolListing` under the sync's own rules and with no allow-list (checked by the new `McpServerBinding::validate_endpoint`), marking a tool a sync would refuse — an oversized schema, a name that cannot form a tool id — instead of refusing the listing, and refusing the whole listing when any server text echoes the credential; a sync names every missing tool at once as `McpSyncError::ToolsNotListed`; and the session, the stored `McpDescriptorSet` (`server_version`, optional on schema version 2), and the listing carry the server's self-reported version. The test kit's `FakeTool::with_annotations` also takes `McpToolAnnotations`, and `FakeMcpServer` gains `with_server_version` (issue #85, asks 1, 2, and 4).
```

- [ ] **Step 4: Crate row** — in `docs/rakka-agents.md`'s `rakka-agent-mcp` row, after "publish-time descriptor sync," insert "a tool listing for choosing tools before any binding names them,". Spec status line: `Status: implemented, 2026-10-11. Branch \`mcp-discovery\` off \`e117ac6\`.`

- [ ] **Step 5: Doc-currency tests**

Run: `cargo test -p rakka-agent --features otel --test compatibility_currency && cargo test -p rakka-testkit --test repository_hygiene`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/rakka-agent-mcp/src/client.rs docs/rakka-compatibility.md CHANGELOG.md docs/rakka-agents.md docs/superpowers/specs/2026-10-11-mcp-discovery-design.md
git commit -F - <<'EOF'
Record issue #85 asks 1, 2, and 4 in the compatibility document and the changelog

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

- [ ] **Step 7: Full validation**

Run in the background, with the log and the exit code written to files in the session scratchpad directory (never piped into `tail`, which would report `tail`'s exit code): `scripts/validate.sh > "$SCRATCH/validate.log" 2>&1; echo "exit $?" > "$SCRATCH/validate.exit"`, where `$SCRATCH` is that directory
Expected: `exit 0`, no warnings. Then on a cold target: `CARGO_TARGET_DIR=target/msrv88-x cargo +1.88.0 check --workspace --all-targets` and `CARGO_TARGET_DIR=target/msrv88-x cargo +1.88.0 check -p rakka-agent-mcp --all-targets --features testkit` — Expected: both finish with no error. Remove `target/msrv88-x` afterwards.
