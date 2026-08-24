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
    AgentAssignmentGeneration, AgentCapabilityId, AgentCredentialBindingRef, AgentDelegationId,
    AgentDelegationRecord, AgentEnvironmentRef, AgentGoalDelegationBudget, AgentGoalId,
    AgentHandoffId, AgentHandoffRecord, AgentId, AgentRevisionNumber, AgentRunId, AgentRunScope,
    AgentSchemaId, AgentSchemaRef, AgentTaskDelegationProvenance, AgentTaskHandoff,
    AgentTaskHandoffRequest, AgentTaskId, KnowledgeSpaceId,
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

/// One versioned handoff envelope: the same-task transfer cluster of the
/// collaboration extension ([specification 8.9](../../../../docs/plans/rakka-agent/spec.md)).
///
/// It rides under the same [`META_COLLABORATION`] key and extension URI as
/// the delegation envelope, discriminated by its `handoff` field. A receiver
/// that predates the cluster fails to parse it — the mandated fail-closed
/// posture for collaboration metadata a peer cannot honor — while ordinary
/// clients that never engage the extension remain untouched. Every field is
/// a *claim* the task's transition re-validates against durable state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct AgentHandoffCollaborationMetadata {
    /// Envelope schema version; must equal
    /// [`AGENT_COLLABORATION_SCHEMA_VERSION`].
    pub schema: u32,
    /// The handoff identity the source run derived.
    pub handoff: String,
    /// The agent the source run claims to be.
    pub source_agent: String,
    /// The source run id claiming the transfer.
    pub source_run: String,
    /// The assignment generation the source claims to serve.
    pub source_generation: u64,
    /// The agent the transfer targets.
    pub target_agent: String,
    /// The task definition the resolved target serves.
    pub target_task_definition: String,
    /// The result schema the resolved target expects, when its catalog entry
    /// declares one.
    #[serde(default)]
    pub result_schema: Option<AgentCollaborationSchemaRef>,
    /// The bounded reason the source's model supplied.
    pub reason: String,
    /// The handoff policy revision that authorized the transfer.
    pub policy_revision: u64,
    /// Explicit context/artifact references projected to the target — never
    /// content, never memory.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<String>,
    /// The communal knowledge spaces the catalog explicitly delegates to the
    /// target. Omitted when empty, with the delegation envelope's
    /// cross-version posture: a grant must never be silently dropped.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub knowledge_spaces: Vec<String>,
}

impl AgentHandoffCollaborationMetadata {
    /// Builds the wire envelope from the handoff record a source run
    /// persisted — the sender side of the cluster.
    #[must_use]
    pub fn from_record(record: &AgentHandoffRecord) -> Self {
        Self {
            schema: AGENT_COLLABORATION_SCHEMA_VERSION,
            handoff: record.handoff.as_str().to_string(),
            source_agent: record.source_run.agent().as_str().to_string(),
            source_run: record.source_run.run().as_str().to_string(),
            source_generation: record.source_generation.get(),
            target_agent: record.resolved.agent.as_str().to_string(),
            target_task_definition: record.resolved.task_definition.as_str().to_string(),
            result_schema: record.resolved.result_schema.as_ref().map(|schema| {
                AgentCollaborationSchemaRef {
                    id: schema.schema_id.as_str().to_string(),
                    revision: schema.version.get(),
                }
            }),
            reason: record.reason.clone(),
            policy_revision: record.policy_revision.get(),
            context: record.context.clone(),
            knowledge_spaces: record
                .resolved
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

    /// Validates the envelope into the typed request the task's transition
    /// re-validates against durable state — the receiver side of the cluster.
    ///
    /// # Errors
    ///
    /// Fails closed on any identity that cannot key a durable scope.
    pub fn to_request(&self) -> RakkaAgentA2AResult<AgentTaskHandoffRequest> {
        let mut knowledge_spaces = std::collections::BTreeSet::new();
        for space in &self.knowledge_spaces {
            knowledge_spaces.insert(KnowledgeSpaceId::new(space)?);
        }
        Ok(AgentTaskHandoffRequest {
            handoff: AgentHandoffId::new(&self.handoff)?,
            source_agent: AgentId::new(&self.source_agent)?,
            source_run: AgentRunId::new(&self.source_run)?,
            source_generation: AgentAssignmentGeneration::new(self.source_generation),
            target: AgentId::new(&self.target_agent)?,
            target_task_definition: rakka_agent::AgentTaskDefinitionId::new(
                &self.target_task_definition,
            )?,
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
            reason: self.reason.clone(),
            policy_revision: AgentRevisionNumber::new(self.policy_revision),
            context: self.context.clone(),
            knowledge_spaces,
        })
    }
}

/// The bounded verb vocabulary of one team cluster
/// ([specification 8.10](../../../../docs/plans/rakka-agent/spec.md)).
///
/// Creation and disband are deliberately absent: a team is trusted
/// application data, and the wire cannot mint or destroy one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentTeamWireOperation {
    /// Claim a board entry for a member.
    Claim,
    /// Release a pending claim.
    Release,
    /// Transfer a pending claim to another member.
    Transfer,
    /// Post an existing task to the board.
    PostTask,
    /// Append a mediated peer message to the durable ring.
    Message,
    /// Add a member, fenced on the lifecycle revision.
    Join,
    /// Remove a member, fenced on the lifecycle revision.
    Leave,
}

impl AgentTeamWireOperation {
    /// Stable kebab-case label for authorization claims and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::Release => "release",
            Self::Transfer => "transfer",
            Self::PostTask => "post-task",
            Self::Message => "message",
            Self::Join => "join",
            Self::Leave => "leave",
        }
    }
}

/// One versioned team cluster: the durable board/membership command shape of
/// the collaboration extension
/// ([specification 8.10](../../../../docs/plans/rakka-agent/spec.md)).
///
/// It rides under the same [`META_COLLABORATION`] key and extension URI as
/// the delegation envelope and the handoff cluster, discriminated by its
/// `team` field — checked *before* the handoff discriminator, and
/// `deny_unknown_fields` makes a payload carrying both fail the send whole.
/// A receiver that predates the cluster fails to parse it (the mandated
/// fail-closed posture); ordinary clients that never engage the extension
/// remain untouched. Every field is a *claim* the team entity's transition
/// re-validates against durable state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct AgentTeamCollaborationMetadata {
    /// Envelope schema version; must equal
    /// [`AGENT_COLLABORATION_SCHEMA_VERSION`].
    pub schema: u32,
    /// The team the command addresses — the cluster's discriminator field.
    pub team: String,
    /// The board or membership verb.
    pub operation: AgentTeamWireOperation,
    /// The board task the operation touches, when it names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,
    /// The member the caller acts as: the claimant, releaser, poster,
    /// sender, or the joining/leaving member.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    /// The member a transfer targets or a message addresses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_member: Option<String>,
    /// The board entry's claim epoch the command observed; a stale
    /// expectation fails closed at the entity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_epoch: Option<u64>,
    /// The lifecycle revision a membership change expects to succeed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_lifecycle_revision: Option<u64>,
    /// The capability scopes a joining member is admitted under.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capability_scopes: Vec<String>,
    /// The bounded message body, on a message append.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

impl AgentTeamCollaborationMetadata {
    /// The JSON value that rides under [`META_COLLABORATION`].
    #[must_use]
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// The bounded verb vocabulary of one conversation cluster
/// ([specification 8.11](../../../../docs/plans/rakka-agent/spec.md)).
///
/// Creation is deliberately absent: a conversation is trusted application
/// data, and the wire cannot mint one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum AgentConversationWireOperation {
    /// Submit the next turn as the current authorized participant.
    SubmitTurn,
    /// End the conversation early under the moderator's policy, fenced on
    /// the round.
    End,
}

impl AgentConversationWireOperation {
    /// Stable kebab-case label for authorization claims and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::SubmitTurn => "submit-turn",
            Self::End => "end",
        }
    }
}

/// One versioned conversation cluster: the moderated turn-protocol command
/// shape of the collaboration extension
/// ([specification 8.11](../../../../docs/plans/rakka-agent/spec.md)).
///
/// It rides under the same [`META_COLLABORATION`] key and extension URI as
/// the other clusters, discriminated by its `conversation` field — checked
/// *before* the team and handoff discriminators, and `deny_unknown_fields`
/// makes a payload carrying more than one discriminator fail the send whole.
/// A receiver that predates the cluster fails to parse it (the mandated
/// fail-closed posture); ordinary clients that never engage the extension
/// remain untouched. Every field is a *claim* the conversation entity's
/// transition re-validates against durable state — the roster gate and the
/// cursor's derived owner fence decide, never the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct AgentConversationCollaborationMetadata {
    /// Envelope schema version; must equal
    /// [`AGENT_COLLABORATION_SCHEMA_VERSION`].
    pub schema: u32,
    /// The conversation the command addresses — the cluster's discriminator
    /// field.
    pub conversation: String,
    /// The turn-protocol verb.
    pub operation: AgentConversationWireOperation,
    /// The claimed speaker of a submitted turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant: Option<String>,
    /// The round a submitted turn claims.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round: Option<u64>,
    /// The turn index a submitted turn claims within its round.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn: Option<u32>,
    /// The bounded turn body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// The tokens the speaker's run reports having consumed producing the
    /// turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens_consumed: Option<u64>,
    /// The roster participant a moderator-directed moderator turn
    /// designates as the next speaker. Mutually exclusive with
    /// [`Self::close_round`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub designate: Option<String>,
    /// Whether a moderator-directed moderator turn closes the round.
    /// Mutually exclusive with [`Self::designate`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_round: Option<bool>,
    /// The round an end decision was made against; a stale expectation
    /// fails closed at the entity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_round: Option<u64>,
    /// The bounded early-end reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl AgentConversationCollaborationMetadata {
    /// The JSON value that rides under [`META_COLLABORATION`].
    #[must_use]
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// One parsed engagement of the collaboration extension: a delegation
/// envelope, a handoff cluster, a team cluster, or a conversation cluster,
/// discriminated by shape under the one [`META_COLLABORATION`] key.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum AgentCollaborationEnvelope {
    /// A delegation send creating a child task.
    Delegation(AgentCollaborationMetadata),
    /// A handoff send transferring responsibility for the same task.
    Handoff(AgentHandoffCollaborationMetadata),
    /// A team board or membership command.
    Team(AgentTeamCollaborationMetadata),
    /// A moderated-conversation turn-protocol command.
    Conversation(AgentConversationCollaborationMetadata),
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
    match parse_collaboration_envelope(message, metadata)? {
        None => Ok(None),
        Some(AgentCollaborationEnvelope::Delegation(envelope)) => Ok(Some(envelope)),
        // A caller expecting only delegation metadata must not silently drop
        // a transfer or a board command: half-understood collaboration
        // metadata fails closed.
        Some(AgentCollaborationEnvelope::Handoff(_)) => Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "a handoff envelope reached a delegation-only surface",
        }),
        Some(AgentCollaborationEnvelope::Team(_)) => Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "a team envelope reached a delegation-only surface",
        }),
        Some(AgentCollaborationEnvelope::Conversation(_)) => Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "a conversation envelope reached a delegation-only surface",
        }),
    }
}

/// Parses the collaboration engagement of one send — delegation envelope,
/// handoff cluster, team cluster, or conversation cluster — failing closed
/// on every half-formed shape.
///
/// Returns `None` for an ordinary send that neither declares the extension
/// nor carries the metadata key. The clusters are discriminated by their
/// fields — `conversation`, then `team`, then `handoff`, with delegation the
/// fallback — and each shape's `deny_unknown_fields` makes a payload
/// carrying more than one discriminator fail the send whole.
///
/// # Errors
///
/// Fails closed when the message declares an unsupported collaboration
/// version, declares v1 without the [`META_COLLABORATION`] object, carries
/// the object without declaring the extension, or carries an envelope that
/// does not parse under schema version 1.
pub fn parse_collaboration_envelope(
    message: &a2a::Message,
    metadata: &std::collections::HashMap<String, Value>,
) -> RakkaAgentA2AResult<Option<AgentCollaborationEnvelope>> {
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
    // `io.rakka.collaboration` carries two directions: this inbound command
    // envelope, and the bounded echo the task projection writes back out.
    // Every cluster shape below requires `schema`; no echo carries one. So
    // the version declaration is checked *before* the discriminators, and
    // that ordering is what keeps this surface from classifying an echo as a
    // command: `team_echo` and `handoff_echo` key their identity on the bare
    // cluster names discriminated on here, and both are shipped
    // compatibility surface that cannot be renamed. Without this gate, a
    // client or relay that round-tripped a task's collaboration object onto
    // a send was refused under whichever cluster its echoes happened to trip
    // first — so the answer it got depended on that task's collaboration
    // history rather than on what it sent, and a task that both handed off
    // and governed a conversation changed branches. Refused either way; the
    // point is that the refusal now names the real defect.
    let declares_schema = payload
        .as_object()
        .is_some_and(|object| object.contains_key("schema"));
    if !declares_schema {
        return Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason: "the collaboration metadata object declares no schema version; a task \
                     projection's collaboration echo is not a collaboration command",
        });
    }
    // The conversation cluster discriminates first, then the team cluster,
    // then the handoff cluster: each shape's `deny_unknown_fields` makes a
    // payload carrying more than one discriminator fail the send whole
    // rather than parse as any of them.
    let is_conversation = payload
        .as_object()
        .is_some_and(|object| object.contains_key("conversation"));
    if is_conversation {
        let envelope: AgentConversationCollaborationMetadata =
            serde_json::from_value(payload.clone()).map_err(|_| {
                RakkaAgentA2AError::Unsupported {
                    operation: "agent-collaboration",
                    reason: "the conversation envelope does not parse under schema version 1",
                }
            })?;
        if envelope.schema != AGENT_COLLABORATION_SCHEMA_VERSION {
            return Err(RakkaAgentA2AError::Unsupported {
                operation: "agent-collaboration",
                reason: "the conversation envelope names an unsupported schema version",
            });
        }
        return Ok(Some(AgentCollaborationEnvelope::Conversation(envelope)));
    }
    let is_team = payload
        .as_object()
        .is_some_and(|object| object.contains_key("team"));
    if is_team {
        let envelope: AgentTeamCollaborationMetadata = serde_json::from_value(payload.clone())
            .map_err(|_| RakkaAgentA2AError::Unsupported {
                operation: "agent-collaboration",
                reason: "the team envelope does not parse under schema version 1",
            })?;
        if envelope.schema != AGENT_COLLABORATION_SCHEMA_VERSION {
            return Err(RakkaAgentA2AError::Unsupported {
                operation: "agent-collaboration",
                reason: "the team envelope names an unsupported schema version",
            });
        }
        return Ok(Some(AgentCollaborationEnvelope::Team(envelope)));
    }
    let is_handoff = payload
        .as_object()
        .is_some_and(|object| object.contains_key("handoff"));
    if is_handoff {
        let envelope: AgentHandoffCollaborationMetadata = serde_json::from_value(payload.clone())
            .map_err(|_| {
            RakkaAgentA2AError::Unsupported {
                operation: "agent-collaboration",
                reason: "the handoff envelope does not parse under schema version 1",
            }
        })?;
        if envelope.schema != AGENT_COLLABORATION_SCHEMA_VERSION {
            return Err(RakkaAgentA2AError::Unsupported {
                operation: "agent-collaboration",
                reason: "the handoff envelope names an unsupported schema version",
            });
        }
        return Ok(Some(AgentCollaborationEnvelope::Handoff(envelope)));
    }
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
    Ok(Some(AgentCollaborationEnvelope::Delegation(envelope)))
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

/// The bounded handoff echo the public task projection carries: handoff
/// identity, status label, and target — enough for observability and the
/// source's identity check past the deduplication window, never the whole
/// record ([specification 8.9](../../../../docs/plans/rakka-agent/spec.md)).
#[must_use]
pub fn handoff_echo(handoff: &AgentTaskHandoff) -> Value {
    serde_json::json!({
        "handoff": handoff.handoff.as_str(),
        "handoff-status": handoff.status.as_label(),
        "handoff-target": handoff.target.as_str(),
        "handoff-target-generation": handoff
            .target_generation
            .map(rakka_agent::AgentAssignmentGeneration::get),
    })
}

/// The bounded team echo the public task projection carries: claim
/// identity, status label, and member — enough for observability and the
/// board's identity check past the deduplication window, never the whole
/// record ([specification 8.10](../../../../docs/plans/rakka-agent/spec.md)).
#[must_use]
pub fn team_echo(claim: &rakka_agent::AgentTaskTeamClaim) -> Value {
    serde_json::json!({
        "team": claim.team.team().as_str(),
        "team-claim": claim.claim.as_str(),
        "team-claim-status": claim.status.as_label(),
        "team-claim-member": claim.member.as_str(),
        "team-claim-epoch": claim.epoch,
        "team-claim-generation": claim
            .target_generation
            .map(rakka_agent::AgentAssignmentGeneration::get),
    })
}

/// The bounded conversation echo the public task projection carries: the
/// latest terminated conversation's identity, terminal status and reason,
/// and its round/turn coordinates — identity and coordinates only, never
/// transcript content
/// ([specification 8.11](../../../../docs/plans/rakka-agent/spec.md)).
///
/// The identity key is `conversation-id`, **not** the bare `conversation`
/// its delegation, handoff, and team siblings use, and that is deliberate:
/// `io.rakka.collaboration` is one key carrying two directions, the outbound
/// projection echo and the inbound command envelope, and
/// [`parse_collaboration_envelope`] discriminates the inbound clusters on
/// exactly those bare names. An echo keyed `conversation` would sit ahead of
/// `team` and `handoff` in that order, so a task's own echo object decided
/// which cluster a round-tripped send was read as. The two older echoes
/// cannot be renamed — they are shipped compatibility surface — so the
/// parser gates on the `schema` no echo carries; this key keeps the newest
/// echo out of the collision to begin with. Any echo added later must do
/// the same.
#[must_use]
pub fn conversation_echo(cell: &rakka_agent::AgentTaskConversation) -> Value {
    serde_json::json!({
        "conversation-id": cell.conversation.as_str(),
        "conversation-status": cell.status.as_label(),
        "conversation-reason": cell.terminal_reason.code(),
        "conversation-rounds": cell.rounds_completed,
        "conversation-turns": cell.turns,
    })
}
