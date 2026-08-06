//! The versioned agent-collaboration metadata extension.
//!
//! A delegation send carries its collaboration context — goal, parent
//! task/run, delegation identity, lineage/depth, requested skill, delegated
//! scopes, advisory budget, deadline, and result contract — as one JSON
//! object under the [`META_COLLABORATION`] metadata key, with
//! [`AGENT_COLLABORATION_EXTENSION_URI`] declared in `message.extensions`
//! (specification 14.4). Metadata, not a data part, because the message's
//! parts *are* the child task's input; the management extension uses a part
//! because its message is the command.
//!
//! Compatibility is asymmetric by design. An ordinary A2A client that never
//! declares the extension is untouched: unknown metadata is ignored, nothing
//! fails closed, and its sends create plain tasks. A message that *does*
//! engage the collaboration surface fails closed on every half-formed shape —
//! an undeclared version of the extension, a declared extension without the
//! metadata object, the metadata object without the declaration, a malformed
//! envelope, or a foreign schema number — because half-understood
//! collaboration metadata would silently sever a child from its delegation
//! graph.
//!
//! The envelope carries logical references only. A credential-binding entry
//! is a binding *reference* resolved by the executing dispatcher's own
//! boundary; resolved credential material never rides this extension
//! (specification 14.4).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use a2a::AgentExtension;
use rakka_agent::{
    AgentCapabilityId, AgentCredentialBindingRef, AgentDelegationId, AgentDelegationRecord,
    AgentEnvironmentRef, AgentGoalDelegationBudget, AgentGoalId, AgentId, AgentRevisionNumber,
    AgentRunScope, AgentSchemaId, AgentSchemaRef, AgentTaskDelegationProvenance, AgentTaskId,
    KnowledgeSpaceId,
};
use rakka_agent_workflow::AgentTimestampMillis;

use super::error::{RakkaAgentA2AError, RakkaAgentA2AResult};

/// Stable URI identifying version 1 of the agent-collaboration extension.
///
/// The version lives in the URI: a breaking envelope change mints a new URI,
/// and a message tagged with an unrecognized collaboration version is refused
/// rather than half-understood.
pub const AGENT_COLLABORATION_EXTENSION_URI: &str = "urn:rakka:a2a-extension:collaboration:v1";

/// URI prefix shared by every version of the collaboration extension, used to
/// detect a request for a version this build does not serve.
pub const AGENT_COLLABORATION_EXTENSION_PREFIX: &str = "urn:rakka:a2a-extension:collaboration:";

/// The wire schema version inside the v1 envelope.
pub const AGENT_COLLABORATION_SCHEMA_VERSION: u32 = 1;

/// Metadata key carrying the collaboration envelope on a send, and the
/// bounded collaboration echo on the public task projection.
pub const META_COLLABORATION: &str = "io.rakka.collaboration";

/// The card-declarable extension descriptor.
#[must_use]
pub fn agent_collaboration_extension() -> AgentExtension {
    AgentExtension {
        uri: AGENT_COLLABORATION_EXTENSION_URI.to_string(),
        description: Some(
            "Versioned Rakka agent collaboration: durable delegation metadata carrying goal, \
             parent, lineage, budget, and deadline context with fail-closed versioning."
                .to_string(),
        ),
        required: Some(false),
        params: None,
    }
}

/// The delegation-ceiling budget the envelope carries.
///
/// A validated cap, never a conserved grant: a real escrow grant cannot ride
/// A2A, so the receiving surface min-narrows the child's own ledger and
/// delegation authority below these ceilings — a peer can only shrink what a
/// child may do, and nothing here debits a ledger across the wire. The
/// `max_descendants` a delegating parent sends is the sub-quota it escrowed
/// parent-side for the child's whole subtree.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct AgentCollaborationBudget {
    /// Maximum delegation depth below the root.
    #[serde(default)]
    pub max_depth: Option<u32>,
    /// Maximum direct children of one delegating run.
    #[serde(default)]
    pub max_fan_out: Option<u32>,
    /// Maximum descendants across the child's whole subtree.
    #[serde(default)]
    pub max_descendants: Option<u32>,
    /// Maximum concurrently unsettled direct children of one delegating run.
    #[serde(default)]
    pub max_concurrent: Option<u32>,
}

impl From<AgentGoalDelegationBudget> for AgentCollaborationBudget {
    fn from(budget: AgentGoalDelegationBudget) -> Self {
        Self {
            max_depth: budget.max_depth,
            max_fan_out: budget.max_fan_out,
            max_descendants: budget.max_descendants,
            max_concurrent: budget.max_concurrent,
        }
    }
}

impl From<AgentCollaborationBudget> for AgentGoalDelegationBudget {
    fn from(budget: AgentCollaborationBudget) -> Self {
        Self {
            max_depth: budget.max_depth,
            max_fan_out: budget.max_fan_out,
            max_descendants: budget.max_descendants,
            max_concurrent: budget.max_concurrent,
        }
    }
}

/// The versioned result-contract reference the envelope carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct AgentCollaborationSchemaRef {
    /// Stable schema identity.
    pub id: String,
    /// Monotonic schema revision.
    pub revision: u64,
}

/// One versioned collaboration envelope.
///
/// Every identity is a plain string on the wire — readable by any A2A client
/// — and validated into its typed form only at the receiving surface, where
/// a value that cannot key a durable scope fails the whole send.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct AgentCollaborationMetadata {
    /// Envelope schema version; must equal
    /// [`AGENT_COLLABORATION_SCHEMA_VERSION`].
    pub schema: u32,
    /// The delegation that creates the child task.
    pub delegation: String,
    /// The parent task whose run delegates.
    pub parent_task: String,
    /// The delegating run's flattened scope key.
    pub parent_run: String,
    /// The collaborative goal the parent serves, when it serves one.
    #[serde(default)]
    pub goal: Option<String>,
    /// The parent's ancestor delegations, oldest first — the chain above
    /// [`Self::delegation`], which is not repeated.
    #[serde(default)]
    pub lineage: Vec<String>,
    /// The agent that committed each lineage entry, oldest first — parallel
    /// to [`Self::lineage`]. Omitted when empty, which keeps a root-level
    /// send parseable by a receiver that predates the field; a deeper chain
    /// carries it and fails closed on such a receiver, exactly the
    /// half-understood-metadata posture of this extension.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ancestors: Vec<String>,
    /// Depth of the child below the root.
    pub depth: u32,
    /// The skill the parent requested.
    pub requested_skill: String,
    /// Capability scopes the parent delegates.
    #[serde(default)]
    pub capability_scopes: Vec<String>,
    /// Logical credential-binding references — never resolved credentials.
    #[serde(default)]
    pub credential_bindings: Vec<String>,
    /// The advisory delegation budget.
    #[serde(default)]
    pub budget: Option<AgentCollaborationBudget>,
    /// The child's deadline, in Unix epoch milliseconds.
    #[serde(default)]
    pub deadline: Option<u64>,
    /// The versioned output schema the parent expects.
    #[serde(default)]
    pub result_schema: Option<AgentCollaborationSchemaRef>,
    /// Environments the delegating scope narrows tool use to. Omitted when
    /// empty, which keeps a scope-free send parseable by a receiver that
    /// predates the field; a send carrying one fails closed on such a
    /// receiver, exactly the half-understood-metadata posture of this
    /// extension.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub environments: Vec<String>,
    /// The communal knowledge spaces explicitly delegated to the child.
    /// Omitted when empty, with the same cross-version posture as
    /// [`Self::environments`]: a grant must never be silently dropped.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub knowledge_spaces: Vec<String>,
}

impl AgentCollaborationMetadata {
    /// Builds the wire envelope from the delegation record a parent
    /// persisted — the sender side of the extension.
    #[must_use]
    pub fn from_record(record: &AgentDelegationRecord) -> Self {
        Self {
            schema: AGENT_COLLABORATION_SCHEMA_VERSION,
            delegation: record.delegation.as_str().to_string(),
            parent_task: record.parent_task.as_str().to_string(),
            parent_run: record.parent_run.key(),
            goal: record.goal.as_ref().map(|goal| goal.as_str().to_string()),
            lineage: record
                .lineage
                .iter()
                .map(|ancestor| ancestor.as_str().to_string())
                .collect(),
            ancestors: record
                .ancestors
                .iter()
                .map(|agent| agent.as_str().to_string())
                .collect(),
            depth: record.depth,
            requested_skill: record.requested_skill.as_str().to_string(),
            capability_scopes: record
                .resolved
                .capability_scopes
                .iter()
                .map(|scope| scope.as_str().to_string())
                .collect(),
            credential_bindings: record
                .resolved
                .credential_bindings
                .iter()
                .map(|binding| binding.as_str().to_string())
                .collect(),
            budget: record.budget.map(AgentCollaborationBudget::from),
            deadline: record.deadline.map(AgentTimestampMillis::as_millis),
            result_schema: record.result_schema.as_ref().map(|schema| {
                AgentCollaborationSchemaRef {
                    id: schema.schema_id.as_str().to_string(),
                    revision: schema.version.get(),
                }
            }),
            environments: record
                .environments
                .iter()
                .map(|environment| environment.as_str().to_string())
                .collect(),
            knowledge_spaces: record
                .knowledge_spaces
                .iter()
                .map(|space| space.as_str().to_string())
                .collect(),
        }
    }

    /// The JSON value that rides under [`META_COLLABORATION`].
    #[must_use]
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// Validates the envelope into the typed provenance the child task
    /// records at creation — the receiver side of the extension.
    ///
    /// # Errors
    ///
    /// Fails closed on any identity that cannot key a durable scope.
    pub fn to_provenance(&self) -> RakkaAgentA2AResult<AgentTaskDelegationProvenance> {
        let parent_run: AgentRunScope =
            serde_json::from_value(Value::String(self.parent_run.clone())).map_err(|_| {
                RakkaAgentA2AError::Unsupported {
                    operation: "agent-collaboration",
                    reason: "the collaboration envelope's parent-run is not a valid run scope key",
                }
            })?;
        let mut lineage = Vec::with_capacity(self.lineage.len());
        for ancestor in &self.lineage {
            lineage.push(AgentDelegationId::new(ancestor)?);
        }
        let mut ancestors = Vec::with_capacity(self.ancestors.len());
        for agent in &self.ancestors {
            ancestors.push(AgentId::new(agent)?);
        }
        let mut capability_scopes = std::collections::BTreeSet::new();
        for scope in &self.capability_scopes {
            capability_scopes.insert(AgentCapabilityId::new(scope)?);
        }
        let mut credential_bindings = Vec::with_capacity(self.credential_bindings.len());
        for binding in &self.credential_bindings {
            credential_bindings.push(AgentCredentialBindingRef::new(binding)?);
        }
        let mut environments = std::collections::BTreeSet::new();
        for environment in &self.environments {
            environments.insert(AgentEnvironmentRef::new(environment)?);
        }
        let mut knowledge_spaces = std::collections::BTreeSet::new();
        for space in &self.knowledge_spaces {
            knowledge_spaces.insert(KnowledgeSpaceId::new(space)?);
        }
        Ok(AgentTaskDelegationProvenance {
            environments,
            knowledge_spaces,
            delegation: AgentDelegationId::new(&self.delegation)?,
            parent_task: AgentTaskId::new(&self.parent_task)?,
            parent_run,
            lineage,
            ancestors,
            depth: self.depth,
            requested_skill: AgentCapabilityId::new(&self.requested_skill)?,
            capability_scopes,
            credential_bindings,
            result_schema: self
                .result_schema
                .as_ref()
                .map(|schema| {
                    Ok::<_, RakkaAgentA2AError>(AgentSchemaRef::new(
                        AgentSchemaId::new(&schema.id)?,
                        AgentRevisionNumber::new(schema.revision),
                    ))
                })
                .transpose()?,
            budget: self.budget.map(AgentGoalDelegationBudget::from),
            deadline: self.deadline.map(AgentTimestampMillis::new),
        })
    }

    /// The validated goal binding the envelope names, when it names one.
    ///
    /// # Errors
    ///
    /// Fails closed on a goal value that cannot key a durable scope.
    pub fn goal_id(&self) -> RakkaAgentA2AResult<Option<AgentGoalId>> {
        self.goal
            .as_deref()
            .map(|goal| Ok(AgentGoalId::new(goal)?))
            .transpose()
    }
}

/// True when the message declares any version of the collaboration extension.
#[must_use]
pub fn is_collaboration_message(message: &a2a::Message) -> bool {
    message
        .extensions
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|uri| uri.starts_with(AGENT_COLLABORATION_EXTENSION_PREFIX))
}

/// Parses the collaboration envelope out of one send, failing closed on
/// every half-formed engagement of the extension.
///
/// Returns `None` for an ordinary send that neither declares the extension
/// nor carries the metadata key — the unknown-optional compatibility of
/// specification 14.4.
///
/// # Errors
///
/// Fails closed when the message declares an unsupported collaboration
/// version, declares v1 without the [`META_COLLABORATION`] object, carries
/// the object without declaring the extension, or carries an envelope that
/// does not parse under schema version 1.
pub fn parse_collaboration_metadata(
    message: &a2a::Message,
    metadata: &std::collections::HashMap<String, Value>,
) -> RakkaAgentA2AResult<Option<AgentCollaborationMetadata>> {
    let extensions = message.extensions.as_deref().unwrap_or_default();
    let declared = extensions
        .iter()
        .any(|uri| uri.starts_with(AGENT_COLLABORATION_EXTENSION_PREFIX));
    let payload = metadata.get(META_COLLABORATION);
    if !declared {
        if payload.is_some() {
            // The reserved key without the declaration is a half-formed
            // engagement, not unknown metadata: silently ignoring it would
            // let a sender believe its collaboration context was recorded.
            return Err(RakkaAgentA2AError::Unsupported {
                operation: "agent-collaboration",
                reason: "collaboration metadata requires the collaboration extension to be \
                         declared on the message",
            });
        }
        return Ok(None);
    }
    let supported = extensions
        .iter()
        .any(|uri| uri == AGENT_COLLABORATION_EXTENSION_URI);
    if !supported {
        return Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "the requested collaboration extension version is not served",
        });
    }
    let Some(payload) = payload else {
        return Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "a collaboration send must carry the io.rakka.collaboration metadata object",
        });
    };
    let envelope: AgentCollaborationMetadata =
        serde_json::from_value(payload.clone()).map_err(|_| RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "the collaboration envelope does not parse under schema version 1",
        })?;
    if envelope.schema != AGENT_COLLABORATION_SCHEMA_VERSION {
        return Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "the collaboration envelope names an unsupported schema version",
        });
    }
    Ok(Some(envelope))
}

/// The bounded collaboration echo the public task projection carries:
/// delegation identity, parent task, and depth — enough for observability
/// and the sender's identity check, never the whole envelope.
#[must_use]
pub fn collaboration_echo(provenance: &AgentTaskDelegationProvenance) -> Value {
    serde_json::json!({
        "delegation": provenance.delegation.as_str(),
        "parent-task": provenance.parent_task.as_str(),
        "depth": provenance.depth,
    })
}
