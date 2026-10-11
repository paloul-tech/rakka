# MCP descriptor set version 2 — a richer stored set that checks itself

Status: design, 2026-10-11. Branch `mcp-descriptor-set-v2` off `a3e5c10`.
Brief: issue #85 (rakka.host A16), ask 3, ask 10, and the ask-10 addendum
(A17), drafted against `96a18b0`; the code was read at `a3e5c10`.

`McpDescriptorSet` is release data: the publish-time sync produces it, a
deployment stores it, and the executor is built from it. Three gaps:

- **Ask 3.** Each stored descriptor keeps the input schema and only the
  *digest* of the output schema, no annotations, and a description that is
  this adapter's fallback text (`MCP tool {tool} on {server}`) when the server
  gave none — so a consumer cannot show what the server declared without a
  second network round, and has to infer "no description" from the fallback's
  wording.
- **Ask 10.** Decode refuses a newer `schema_version` and validates the server
  id and each binding, but not a descriptor against itself: a descriptor of the
  wrong kind, a tool id other than the one derived from the server and tool
  names, a `schema_digest` that is not the digest of its own `input_schema`,
  and inline `parameters` that differ from `input_schema` all decode. A set is
  release data a client may author, so the registry could register a
  model-visible schema the dispatch-time recheck never compares.
- **The addendum.** Decode admits two descriptors under one tool name, and the
  crate's readers of a set disagree about which wins: `descriptor()` and the
  executor's routing table take the first, `mcp_descriptor_staleness`'s
  `shapes` keeps the last, and `digest()` hashes both.

## Decision 1 — version 2 adds three fields per tool

`MCP_DESCRIPTOR_SET_SCHEMA_VERSION` becomes **2**. The sync writes 2; decode
reads 1 and 2 and refuses anything newer, as today. `McpSyncedDescriptor`
gains:

```rust
/// The server's raw output schema, when it declared one.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub output_schema: Option<Value>,
/// The tool annotations the server reported, when it reported any.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub annotations: Option<McpToolAnnotations>,
/// Whether the descriptor's description is the server's own (`true`) or this
/// adapter's fallback (`false`); `None` on a version-1 set, which did not
/// record it.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub description_supplied: Option<bool>,
```

A version-1 set decodes with all three absent. The sync fills them on every
set it writes: `output_schema` when the server declared one,
`annotations` when the server reported any, `description_supplied` always.

`McpToolAnnotations` is owned by this crate, so rmcp's serde shape never
becomes the stored format:

```rust
pub struct McpToolAnnotations {
    pub title: Option<String>,
    pub read_only_hint: Option<bool>,
    pub destructive_hint: Option<bool>,
    pub idempotent_hint: Option<bool>,
    pub open_world_hint: Option<bool>,
}
```

The `title` is server-chosen text that now becomes release data, so the sync
treats it as it treats a description: the credential-echo rule refuses a
title carrying the credential the sync sent
(`mcp-descriptor-credential-echoed`, field `annotations title`), and a kept
title is cut at `AGENT_TOOL_DESCRIPTION_MAX_LENGTH` on a character boundary.
The raw output schema is already bounded by the sync at
`MCP_DESCRIPTOR_SCHEMA_MAX_BYTES` and already checked for an echoed
credential; the sync now keeps it rather than only its digest.

`McpDescriptorSet::digest()` and `mcp_descriptor_staleness` are **unchanged**:
the digest stays over each tool's name and its two schema digests, so a set
re-synced into version 2 pins the same release digest it pinned as version 1,
and annotations and the description flag pin nothing.

**Rollout.** A build before this one refuses a version-2 set (its decode
refuses a newer version). A deployment upgrades every reader of its stored
sets before it publishes with this build. A version-1 set keeps decoding.

## Decision 2 — a set checks itself

`McpDescriptorSet::validate(&self) -> Result<(), McpRegistrationError>`
refuses, at the first failure:

1. two descriptors with one `tool` name (the addendum), in any order;
2. a descriptor whose binding's kind is not `AgentToolKind::RemoteMcp`;
3. a descriptor whose binding's tool id is not `mcp.<server_id>.<tool>` — the
   derivation `McpServerBinding::tool_id` uses;
4. a `schema_digest` that is not `AgentContentDigest::sha256_of_json` of
   `input_schema`;
5. inline `parameters` that are present and differ from `input_schema`
   (absent parameters are allowed: the sync omits them for a schema over the
   inline bound);
6. on a version-2 set, an `output_schema_digest` that is not the SHA-256 of
   `output_schema` — both present and agreeing, or both absent. A version-1 set
   has no `output_schema` to compare, so its digest is not checked.

The refusal is a new `McpRegistrationError::DescriptorSetInvalid { server,
tool: Option<String>, reason }` under a new stable code,
**`mcp-descriptor-set-invalid`**, the eighteenth MCP code; `reason` is a
fixed phrase naming the check, never server-chosen text.

**Where it runs — strict on every decode, whatever the version** (the owner's
ruling):

- **Decode.** `McpDescriptorSet` decodes through a private record and then
  `validate()` (the `#[serde(try_from = ..)]` pattern `AgentToolBinding`
  already uses in this crate); a failing set does not decode, and the serde
  error text leads with the code.
- **Executor construction.** `McpDispatchToolExecutor::new` and
  `with_launcher` call `validate()` on every set they are given, so a set built
  in code rather than decoded is held to the same rule.
- **Callers.** `validate()` is public.

**Cost of strictness, stated.** A stored set that fails a check stops
decoding at the build that carries this — in particular a release published
between rakka.host's A16 (which added its publish-time checks for 1–5) and A17
(which added its duplicate-name check) could hold a duplicate tool. The host
repairs such a release before or at the pin bump.

## Decision 3 — the readers agree

With duplicates refused at decode and at executor construction, no route,
lookup, or staleness check meets two copies of a tool. As belt and braces,
`mcp_descriptor_staleness`'s `shapes` keeps the **first** descriptor for a tool
name, as `descriptor()` and the executor's routing table already do, so even
an unvalidated in-memory set is read alike by all three.

## What this does not change

- No other ask of #85. Ask 11's testkit gaps are not filled beyond what these
  proofs need (the fake already supports `with_annotations`,
  `with_output_schema`, and an empty description).
- Decode checks consistency, not size: schema sizes stay bounded by the sync,
  not re-checked on decode.
- `digest()`, `mcp_descriptor_staleness`'s result, the recheck, and the
  executor's dispatch behavior.

## Proof

Each test is written to fail first against `a3e5c10`.

- **Sync** (against the testkit's fake server): a synced set is version 2 and
  carries a tool's annotations (`FakeTool::with_annotations`), its raw output
  schema (`with_output_schema`), and `description_supplied: Some(true)`; a tool
  with an empty description carries the fallback text and
  `description_supplied: Some(false)`; an annotation title that echoes the
  credential is refused `mcp-descriptor-credential-echoed`.
- **Decode:** each of the six checks refuses its broken set with
  `mcp-descriptor-set-invalid`, the duplicate case in both orders; a version-1
  set (the shape `a3e5c10` writes) still decodes, with the three fields absent;
  a set newer than version 2 is still refused; a synced version-2 set
  round-trips.
- **Executor:** construction refuses an in-memory set that fails a check.
- **Readers:** for an unvalidated in-memory set with a duplicated tool whose
  two copies differ, `descriptor()` and `mcp_descriptor_staleness` read the
  same (first) copy — a fresh set equal to the first copy is `Fresh`, one
  equal to the second is `Stale` — and executor construction refuses the set,
  so the routing table never meets it.
- **Digest stability:** a set's `digest()` is the same whether written as
  version 1 or version 2.

Documentation: `docs/rakka-compatibility.md` (version 2, its three fields,
`McpToolAnnotations`, the new code, strict decode and its cost, the rollout
rule) and its schema-version table (a `MCP_DESCRIPTOR_SET_SCHEMA_VERSION` row
under `rakka-agent-mcp`, held by `compatibility_currency.rs`), `CHANGELOG.md`,
and a status comment on #85 after merge.
