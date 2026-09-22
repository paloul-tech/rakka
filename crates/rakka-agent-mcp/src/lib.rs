//! Remote MCP servers as Rakka tools, under three conditions the host holds
//! as invariants: the transport client is injected and every attempt's
//! server URL passes a required egress check before a client exists; no
//! child process runs without a deployment-supplied launcher; descriptors are
//! release data synced at publish time, and the only credential path is the
//! binding's logical reference resolved by the dispatcher inside the attempt.
//!
//! Module map:
//! - `binding`: server ids, transports, tool policies, the binding record, and
//!   its refusals.
//! - `client`: per-attempt rmcp client construction shared by the sync and the
//!   executor — credential to header, protocol negotiation, the peer-agent
//!   rule, error mapping.
//! - `sync`: `sync_mcp_descriptors`, the stored descriptor set, hint
//!   narrowing, staleness.
//! - `executor`: `McpDispatchToolExecutor`, the egress check, result mapping.
//! - `launcher`: the child-process seam and (feature `child-process`) the
//!   unsandboxed reference launcher.
//! - `testkit` (feature `testkit`): the in-process fake server and counting
//!   client.
//!
//! MCP is never an agent-to-agent channel ([specification 14.4]); a server
//! that identifies as a Rakka agent is refused.
//!
//! [specification 14.4]: ../../../docs/plans/rakka-agent/spec.md

#![forbid(unsafe_code)]

pub mod binding;
pub mod client;
pub mod executor;
pub mod launcher;
pub mod sync;
#[cfg(feature = "testkit")]
pub mod testkit;

pub use binding::{
    McpDescriptorRefresh, McpRegistrationError, McpServerBinding, McpServerId, McpToolPolicy,
    McpTransport, MCP_CLIENT_NAME, MCP_DEFAULT_PROTOCOL_VERSIONS,
    MCP_DESCRIPTOR_RECHECK_TTL_DEFAULT_MS, MCP_DESCRIPTOR_SCHEMA_MAX_BYTES,
    MCP_INLINE_RESULT_MAX_BYTES, MCP_META_IDEMPOTENCY_KEY, MCP_PEER_AGENT_SERVER_PREFIX,
    MCP_TOOL_ERROR_DETAIL_MAX_BYTES, MCP_TOOL_ID_PREFIX,
};
pub use client::{McpClientError, McpClientSession};
pub use sync::{
    mcp_descriptor_staleness, sync_mcp_descriptors, McpDescriptorSet, McpDescriptorStaleness,
    McpSyncError, McpSyncedDescriptor,
};
