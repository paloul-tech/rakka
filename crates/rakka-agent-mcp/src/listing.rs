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
//! [`MCP_DESCRIPTOR_SCHEMA_MAX_BYTES`], or a name that cannot form a tool id
//! or the schema ids a sync derives from it —
//! is listed and marked [`McpListedTool::unusable`], its schemas withheld, so
//! one such tool never hides the rest.
//!
//! Every piece of server text the listing received — the server's name and
//! version, and each listed tool's name, description, title and schemas,
//! withheld or not — is judged first, and one that carries the credential the
//! call sent refuses the whole listing (`mcp-descriptor-credential-echoed`).

use std::collections::BTreeMap;
use std::time::Duration;

use rakka_agent::{AgentContentDigest, AgentToolId, AGENT_IDENTITY_MAX_LENGTH};
use rakka_agent_workflow::AgentEphemeralCredential;
use rmcp::model::Tool;
use rmcp::transport::streamable_http_client::StreamableHttpClient;
use serde_json::{Map, Value};

use crate::binding::{
    McpRegistrationError, McpServerBinding, McpServerId, MCP_DESCRIPTOR_SCHEMA_MAX_BYTES,
    MCP_SYNC_TIMEOUT_DEFAULT_MS,
};
use crate::client::{connect, credential_secrets, service_error, McpEgressCheck};
use crate::sync::{
    bounded_description, bounded_server_name, carries, cut_at, judge_server_identity,
    pages_unended, schema_ref, stored_annotations, sync_deadline, timed_out, value_carries,
    McpSyncError, McpToolAnnotations,
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
    /// [`AGENT_IDENTITY_MAX_LENGTH`] when it cannot be bound.
    pub name: String,
    /// `mcp.<server>.<name>`, when the name can be bound: it and the schema
    /// ids a sync derives from it are all valid identifiers.
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
    /// The name cannot form a valid tool id, or one of the schema ids a sync
    /// derives from it (`mcp.<server>.<name>.input` and `.output`).
    NameInvalid {
        /// The identifier rule's own reason, which names at most one
        /// offending character, escaped.
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
    // The material the server now holds, and so could echo into anything it
    // lists.
    let secrets = credential_secrets(credential);
    let listed = tokio::time::timeout_at(deadline, session.list_all_tools()).await;
    let protocol_version = session.negotiated_version().as_str().to_string();
    let reported_name = session.server_name().to_string();
    let reported_version = session.server_version().map(str::to_string);
    // Closed before the answer is judged, as the sync closes its session.
    session.close().await;
    // Judged first, as the sync judges it: a server that echoes the
    // credential into its identity is refused for that, whatever else its
    // answer got wrong.
    judge_server_identity(
        &server,
        &reported_name,
        reported_version.as_deref(),
        &secrets,
    )?;
    let listed = listed
        .map_err(|_| timed_out(&server, timeout))?
        .map_err(|error| McpSyncError::Client(service_error(&server, &error)))?
        .ok_or_else(|| pages_unended(&server))?;
    // Every tool the server listed, every copy, before anything is built.
    for tool in &listed {
        judge_listed_tool(&server, tool, &secrets)?;
    }
    Ok(McpToolListing {
        server_id: binding.server_id.clone(),
        server_name: bounded_server_name(&reported_name),
        server_version: reported_version.as_deref().map(bounded_server_name),
        protocol_version,
        tools: listed_tools(binding, &listed),
    })
}

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
    let tool_id = match bindable_tool_id(binding, name) {
        Ok(tool_id) => tool_id,
        Err(reason) => {
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

/// The tool id a sync would register `name` under, or the identifier rule's
/// reason a sync would refuse it for: its tool id, or either schema id the
/// sync derives from it — `mcp.<server>.<name>.input` and `.output`, the
/// longer of the three — is not a valid identifier. The same derivations the
/// sync runs, so a name the listing calls bindable is one a sync accepts.
fn bindable_tool_id(binding: &McpServerBinding, name: &str) -> Result<AgentToolId, String> {
    let tool_id = binding.tool_id(name).map_err(|refusal| match refusal {
        McpRegistrationError::ToolNameInvalid { reason, .. } => reason,
        other => other.to_string(),
    })?;
    let server = binding.server_id.to_string();
    for side in ["input", "output"] {
        schema_ref(&server, name, side).map_err(|refusal| match refusal {
            McpSyncError::Descriptor { reason, .. } => reason,
            other => other.to_string(),
        })?;
    }
    Ok(tool_id)
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
