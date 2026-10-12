# MCP discovery — a public tool listing, every missing name, and the server's version

Status: implemented, 2026-10-11. Branch `mcp-discovery` off `e117ac6`.
Brief: issue #85 (rakka.host A16), asks 1, 2 and 4, drafted against
`96a18b0`; the code was read at `e117ac6`. Folded in by the owner: an
rmcp-free `FakeTool::with_annotations`, offered in the #85 status for asks 5,
8, 9, 11 and 13.

rakka.host's discovery route runs `sync_mcp_descriptors_within` for the tools
Studio names, and projects the stored set it answers. Three gaps cost it a
workaround:

- **Ask 1, no public listing.** A full listing exists only inside the crate:
  `McpClientSession` and its `list_all_tools` are `pub(crate)`, and the public
  sync checks only the names a binding's allow-list carries. The host reports
  `discovery: named` and the degradation `discovery_named_only`. Studio cannot
  browse a server.
- **Ask 2, the first missing name only.** `synced_descriptors` collects a
  `Result` per allow-listed tool, so the sync stops at the *first* name the
  listing lacks (`Descriptor { reason: "the server does not list it" }`), and
  Studio re-asks once per missing tool.
- **Ask 4, no server version.** The session keeps the server's self-reported
  name and drops its version (`server_info.version`); neither the stored set
  nor the host's projection can show one.

And one from the test kit: `FakeTool::with_annotations` takes rmcp's own
`ToolAnnotations`, so a host proof that sets annotations needs a direct rmcp
dependency, which the host dropped with ask 9.

## Decision 1 — `list_mcp_tools`, a listing under the sync's rules

Two new public functions, beside the sync:

```rust
pub async fn list_mcp_tools<C>(
    http: &C,
    binding: &McpServerBinding,
    credential: Option<&AgentEphemeralCredential>,
    egress: &dyn McpEgressCheck,
) -> Result<McpToolListing, McpSyncError>
where
    C: StreamableHttpClient + Sync;

pub async fn list_mcp_tools_within<C>(
    http: &C,
    binding: &McpServerBinding,
    credential: Option<&AgentEphemeralCredential>,
    egress: &dyn McpEgressCheck,
    timeout: Duration,
) -> Result<McpToolListing, McpSyncError>
where
    C: StreamableHttpClient + Sync;
```

`list_mcp_tools` applies `MCP_SYNC_TIMEOUT_DEFAULT_MS`; the `_within` twin
takes the caller's bound, which covers the handshake and the listing
together, and the call returns within it plus `MCP_SESSION_CLOSE_BOUND_MS`.
Errors are `McpSyncError` under its existing codes; a listing that runs out of
time is `mcp-descriptor-sync-failed`, as a sync is.

**The binding's allow-list is ignored**, and may be empty. The binding
supplies the server id, the endpoint, the credential binding, and the
protocol versions — what a connection needs — and nothing else is read from
it. So the listing validates only that half, through a new public method:

```rust
impl McpServerBinding {
    /// Refuses a binding no connection can be made from: an endpoint URL
    /// that fails the URL rule, a child-process binding that names a
    /// server-level credential binding, and a protocol-version list that is
    /// empty, malformed, or names a version the pinned SDK does not know.
    /// The tool allow-list is not read.
    pub fn validate_endpoint(&self) -> Result<(), McpRegistrationError>;
}
```

`validate()` keeps every check it makes today, in the same order, so which
refusal a binding with several faults reports does not change; the URL and
version checks move into helpers both methods call. A host validates a
browse request's tool-less binding with `validate_endpoint()` exactly as it
validates a named request's with `validate()`.

**What runs, in order** — the sync's own path, from the same helpers:

1. `binding.validate_endpoint()`.
2. `connect`: the egress check fires before a client exists; the credential
   is read only below it; the handshake refuses a peer that names itself a
   Rakka agent (`mcp-peer-agent-channel-refused`) and a version the binding
   does not list (`mcp-protocol-unsupported`). A child-process binding is
   refused there as no HTTP endpoint (`mcp-descriptor-sync-failed`); there
   is no `list_mcp_tools_over` — the one deployment that asked never enables
   `child-process`, and the twin can follow the sync's pattern when one does.
3. One `tools/list`, following cursors for at most `MCP_LIST_PAGES_MAX`
   pages, under the deadline.
4. The session is closed, on every path, before the answer is judged.
5. The credential-echo judgment (below), then the listing is built.

Nothing is stored and no binding is minted: a listing is what a caller shows
a person choosing tools, and the sync stays the only producer of release
data.

## Decision 2 — the listing's shape

```rust
/// What one MCP server lists, vetted but not bound.
#[derive(Debug, Clone, PartialEq)]
pub struct McpToolListing {
    /// The server listed.
    pub server_id: McpServerId,
    /// The server's self-reported name, cut at `MCP_SERVER_NAME_MAX_BYTES`.
    pub server_name: String,
    /// The server's self-reported version, cut the same way; `None` when it
    /// reported none (Decision 4).
    pub server_version: Option<String>,
    /// The protocol version the listing negotiated.
    pub protocol_version: String,
    /// One entry per tool name, in tool-name order.
    pub tools: Vec<McpListedTool>,
}

/// One tool as the server lists it.
#[derive(Debug, Clone, PartialEq)]
pub struct McpListedTool {
    /// The server's own name for the tool.
    pub name: String,
    /// `mcp.<server>.<name>`, when that forms a valid tool id.
    pub tool_id: Option<AgentToolId>,
    /// The server's description, cut at `AGENT_TOOL_DESCRIPTION_MAX_LENGTH`
    /// on a character boundary; `None` when it gave none or an empty one.
    pub description: Option<String>,
    /// The tool's hints, the title cut as the description is.
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
    /// An input or output schema is over `MCP_DESCRIPTOR_SCHEMA_MAX_BYTES`.
    /// `code()` is `mcp-descriptor-schema-too-large`.
    SchemaTooLarge {
        /// The larger offending schema's encoded size.
        bytes: usize,
    },
    /// The name cannot form a valid tool id, or a schema id a sync derives
    /// from it (`mcp.<server>.<name>.input`, `.output`). `code()` is
    /// `mcp-binding-invalid`.
    NameInvalid {
        /// The identifier rule's own reason; it names at most one offending
        /// character, escaped.
        reason: String,
    },
}
```

`McpListedToolRefusal::code()` answers the code a sync naming the tool would
be refused under, so a caller can show the same code either way. Precedence
when both apply: `NameInvalid`, because a name that cannot be bound makes its
schemas moot.

**A tool that fails a per-tool bound is marked, not refused** (owner ruling):
it comes back with `unusable` set, `input_schema`, `schema_digest`,
`output_schema` and `output_schema_digest` all `None`, and its name,
description and annotations as for any tool. Every other tool still lists. A
`NameInvalid` tool's `name` is cut at `AGENT_IDENTITY_MAX_LENGTH` (256 bytes)
on a character boundary — the server chooses its length and a name that
cannot be bound has nothing to match against — and `tool_id` is `None`; a
bindable tool's name is returned whole, as its id already bounds it.

**Order and duplicates.** Entries are sorted by the server's name for the
tool. A name the server lists twice yields one entry, from its first copy, as
`synced_descriptors` matches an allow-listed name to its first copy (`find`).

**No hint check.** A listing has no operator policy to contradict, so a hint
is reported, never judged; `honor_hints` applies when the sync runs.

**Structs stay exhaustive**, like `McpDescriptorSet`: a caller's tests can
build a listing by literal. The refusal enum is `#[non_exhaustive]`, so a
later per-tool bound is additive.

## Decision 3 — an echoed credential refuses the whole listing

Every piece of server text the listing received is judged before anything is
built, and any one that carries the credential the call sent refuses the
listing as `McpSyncError::CredentialEchoed` (`mcp-descriptor-credential-echoed`),
naming the field:

- the server's name (`server name`, `tool: None`) and version
  (`server version`, `tool: None`), judged first;
- then each tool the server listed — every copy, duplicates included, in the
  server's order — on its name (`tool name`), description, annotation title,
  input schema and output schema, judged raw and whole, including a schema
  the listing would withhold as too large.

"Every byte the listing received" is the rule because it is the one with no
exceptions to reason about: a server that reflects the credential into any of
its text is refused for that, whatever it was going to be shown as. The
sync's own judgment is unchanged except for the version (Decision 4): it
judges the allow-listed tools it builds, as today.

A credential-free listing (`credential: None`) judges nothing, as a
credential-free sync does. `CredentialEchoed`'s documented field list gains
`server version` and `tool name`; the variant's shape is unchanged.

## Decision 4 — the server's version

- **The session** keeps `server_info.version` beside the name, from the same
  `peer_info` the handshake reads. An absent `server_info` or an empty
  version reads as no version.
- **The stored set** gains a field, on schema version **2** with no bump
  (owner ruling):

  ```rust
  /// The server's self-reported implementation version at sync time, cut at
  /// `MCP_SERVER_NAME_MAX_BYTES`; `None` when it reported none, or on a set
  /// written before the field existed.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub server_version: Option<String>,
  ```

  It is display data: not in `digest()`, not in `validate()`, not compared by
  `mcp_descriptor_staleness`. The private decode record gains it as an
  `Option` too. A reader at `317463d`–`e117ac6` decodes a set that carries
  it — the record has no `deny_unknown_fields` — and drops it, which costs
  nothing a decision reads; slice 7.10's display-only fields set the
  precedent.
- **One bound** for the server's identity text: the version is cut at
  `MCP_SERVER_NAME_MAX_BYTES` (256) on a character boundary, as the name is;
  no new constant.
- **The echo rule** covers it: a sync or a listing whose server version
  carries the credential is refused, `field: "server version"`, judged right
  after the name.
- **Source-breaking, compile-time only:** `McpDescriptorSet` is not
  `#[non_exhaustive]`, so a struct literal of it adds `server_version: None`
  (rakka.host's catalog test kit has one, `testkit.rs:526`).
- **The fake** gains `FakeMcpServer::with_server_version(..)`; today it
  reports `"fake"`.

## Decision 5 — every missing name at once

A new variant, on the `#[non_exhaustive]` `McpSyncError`:

```rust
/// The server does not list tools the binding's allow-list names.
ToolsNotListed {
    /// The server listed.
    server: String,
    /// Every allow-listed tool the listing lacks, in tool-name order.
    tools: Vec<String>,
},
```

`code()` is `mcp-binding-invalid`, the code the single-tool refusal had, so
no code changes meaning. Display: `the MCP server {server} does not list the
tools {a}, {b}, …` — the names are the binding's own, already validated.

**Order of a sync's refusals** after the listing: an echoed credential in the
server's name, then in its version; then every allow-listed name the listing
lacks, all at once; then the per-tool checks for the tools it has, in
tool-name order, as today. The single-tool `Descriptor { reason: "the server
does not list it" }` no longer occurs. A caller that matched on it by reason
text reads the new variant instead; rakka.host's `sync_outcome` maps it
through its catch-all arm until it names the tools itself.

## Decision 6 — rmcp-free annotations on the fake

`FakeTool::with_annotations` becomes generic:

```rust
pub fn with_annotations(mut self, annotations: impl Into<ToolAnnotations>) -> Self
```

and the test kit adds `impl From<McpToolAnnotations> for ToolAnnotations`
(through rmcp's `ToolAnnotations::from_raw`, as rmcp's type is
`#[non_exhaustive]`). A caller passing rmcp's own type compiles unchanged; a
deployment passes `McpToolAnnotations` with no rmcp dependency. The
`FakeTool.annotations` field keeps rmcp's type — it is what the fake lists —
and the impl lives under `testkit`, so the crate's default surface gains no
rmcp-typed impl.

## What this does not change

- The sync's output for a server that lists every named tool: the same set,
  digest and staleness, plus `server_version`.
- `McpDescriptorSet::digest()`, `validate()`, `mcp_descriptor_staleness`, and
  the schema version.
- Any error code, and what any existing code means.
- `McpServerBinding::validate()`'s checks and their order.
- The executor; a dispatch never lists.

## What rakka.host can change at the pin bump

| Ask | Change |
| --- | --- |
| 1 | A browse mode on the discovery route over `list_mcp_tools_within`, its tool-less binding checked by `validate_endpoint`; the block reports `named_or_browse` and drops `discovery_named_only` |
| 2 | The route's `mcp-binding-invalid` detail names every tool in `ToolsNotListed::tools` (a contract decision with Studio first) |
| 4 | The projection shows `server_version`, from the set and the listing |
| fake | Proofs set annotations through `McpToolAnnotations` with no rmcp dependency |

Compile-time, at the bump: the catalog test kit's `McpDescriptorSet` literal
adds `server_version: None` (and, from #90, its `McpSyncedDescriptor` literal
adds three `None`s).

## Proof

Each test is written first and seen failing. Against the in-process fake:

- **Listing:** a browse of a server whose tools include a healthy one, one
  with an oversized input schema, one with an oversized output schema, one
  whose name cannot form an id (too long, and one with a control character),
  and one with no description: one entry each, in name order, the healthy one
  carrying both schemas and their digests, the oversized ones marked
  `SchemaTooLarge` with the larger size and no schemas, the misnamed ones
  `NameInvalid` with the name cut and no id, the undescribed one with
  `description: None`.
- **The allow-list is ignored:** a binding with no tools lists, and one whose
  allow-list names a tool the server lacks lists without refusal.
- **`validate_endpoint`** refuses a bad URL, an empty, malformed, or unknown
  version, and a child-process binding with a server credential binding, and
  accepts a tool-less binding `validate()` refuses `NoTools`.
- **Rules carried over:** the egress refusal arrives before any client (the
  counting client sends nothing); a peer-agent name is refused; the listing
  is bounded in time and closes its session (the fake's `list_delay`); a
  listing whose pages never end is refused; a duplicated name yields one
  entry, from the first copy.
- **Echo:** one test per field — server name, server version, tool name,
  description, annotation title, input schema, output schema, an oversized
  (withheld) schema, and a second copy of a duplicated tool — each refusing
  the whole listing under `mcp-descriptor-credential-echoed` with that field.
- **Ask 2:** a binding naming three tools of which two are missing is refused
  `ToolsNotListed` with both names in order; a missing name wins over a
  per-tool refusal of a present tool; an echo in the server's name wins over
  a missing name.
- **Ask 4:** a sync and a listing carry the fake's version; a long version is
  cut on a character boundary; an echoed version refuses both; a set without
  the field decodes with `None`, and one with it round-trips; the digest of a
  set is the same with and without a version.
- **Fake:** `with_annotations(McpToolAnnotations { .. })` lists the same
  annotations as the rmcp-typed call, and the sync stores them.

Docs: a compatibility-document bullet (the new public names, the new variant,
the set field and its literal break, the listing's rules), a `CHANGELOG.md`
entry, and the crate's module docs, including `McpClientSession`'s list of the
public ways to reach a server. Validation: `scripts/validate.sh` and a cold
`cargo +1.88.0 check --workspace --all-targets`.

## Amendment after the final review

The listing's name rule first named only the tool id, `mcp.<server>.<name>` at
most 256 bytes. A sync also derives the schema ids
`mcp.<server>.<name>.input` and `.output` under the same 256-byte identifier
rule and refuses a name either one exceeds, so for server `crm` a name of
242–248 bytes was listed bindable and then refused at publish. `NameInvalid`
now covers all three derivations, through the sync's own `schema_ref`, and
`tool_id` is `None` whenever a name is `NameInvalid`; for `crm` the last
bindable length is 241 bytes, which the boundary test holds against both the
listing and a real sync.
