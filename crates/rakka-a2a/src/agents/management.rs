//! The versioned agent-management extension.
//!
//! Settings and administrative commands enter the A2A surface through a
//! versioned protocol extension rather than ordinary task messages or
//! internal actor remoting (specification 7.2 and 14.1; resolved open
//! decision 10). The extension is identified by [`AGENT_MANAGEMENT_EXTENSION_URI`];
//! a command is a `message/send` whose single data part is the
//! [`AgentManagementRequest`] envelope and whose message is tagged with the
//! URI. An unsupported or unknown management version fails closed.
//!
//! An accepted command answers with an immediate message — never an A2A
//! task: a settings command is not a unit of work, and admitting it into
//! task identity would put administrative operations into the public task
//! projection (specification 14.2). The response carries the applied
//! revisions, the original outcome for a deduplicated retry, or the
//! stale-revision conflict the caller must rebase on. The authenticated
//! principal becomes the [`AgentRevisionProvenance`] of every accepted
//! revision; commands enter through the durable deduplicated inbox of the
//! owning agent entity.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use a2a::{AgentExtension, Message, Part, PartContent, Role};
use rakka_agent::{
    AgentEntityOutcome, AgentEntitySnapshot, AgentRevisionNumber, AgentRevisionProvenance,
    AgentSettingsChange,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};

use super::error::{RakkaAgentA2AError, RakkaAgentA2AResult};

/// Stable URI identifying version 1 of the agent-management extension.
///
/// The version lives in the URI: a breaking payload change mints a new URI,
/// and a request tagged with an unrecognized management version is refused
/// rather than half-understood.
pub const AGENT_MANAGEMENT_EXTENSION_URI: &str = "urn:rakka:a2a-extension:agent-management:v1";

/// URI prefix shared by every version of the management extension, used to
/// detect a request for a version this build does not serve.
pub const AGENT_MANAGEMENT_EXTENSION_PREFIX: &str = "urn:rakka:a2a-extension:agent-management:";

/// The wire schema version inside the v1 envelope.
pub const AGENT_MANAGEMENT_SCHEMA_VERSION: u32 = 1;

/// Metadata key carrying the caller's immutable audit reference for a
/// management write; absent, the audit reference derives from the message id.
pub const META_AUDIT_REF: &str = "io.rakka.audit.ref";

/// The card-declarable extension descriptor.
#[must_use]
pub fn agent_management_extension() -> AgentExtension {
    AgentExtension {
        uri: AGENT_MANAGEMENT_EXTENSION_URI.to_string(),
        description: Some(
            "Versioned Rakka agent management: settings updates and lifecycle commands \
             with monotonic revision fencing."
                .to_string(),
        ),
        required: Some(false),
        params: None,
    }
}

/// One versioned management request envelope.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub struct AgentManagementRequest {
    /// Envelope schema version; must equal [`AGENT_MANAGEMENT_SCHEMA_VERSION`].
    pub schema: u32,
    /// The command to apply.
    pub command: AgentManagementCommand,
}

/// One management command, addressed to an agent by id within the
/// authenticated tenant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
#[non_exhaustive]
pub enum AgentManagementCommand {
    /// Apply a settings update against an expected current revision
    /// (specification 7.2).
    UpdateSettings {
        /// Target agent id.
        agent: String,
        /// Settings revision the caller believes is current.
        expected_revision: AgentRevisionNumber,
        /// Field-level changes to apply.
        changes: Vec<AgentSettingsChange>,
    },
    /// Suspend the agent before any further dispatch.
    Suspend {
        /// Target agent id.
        agent: String,
        /// Lifecycle revision the caller believes is current.
        expected_lifecycle_revision: AgentRevisionNumber,
    },
    /// Resume a suspended agent.
    Resume {
        /// Target agent id.
        agent: String,
        /// Lifecycle revision the caller believes is current.
        expected_lifecycle_revision: AgentRevisionNumber,
    },
    /// Permanently retire the agent.
    Terminate {
        /// Target agent id.
        agent: String,
        /// Lifecycle revision the caller believes is current.
        expected_lifecycle_revision: AgentRevisionNumber,
    },
    /// Read the agent's bounded durable projection.
    Describe {
        /// Target agent id.
        agent: String,
    },
}

impl AgentManagementCommand {
    /// The target agent id.
    #[must_use]
    pub fn agent(&self) -> &str {
        match self {
            Self::UpdateSettings { agent, .. }
            | Self::Suspend { agent, .. }
            | Self::Resume { agent, .. }
            | Self::Terminate { agent, .. }
            | Self::Describe { agent } => agent,
        }
    }

    /// True when the command mutates durable state.
    #[must_use]
    pub const fn is_write(&self) -> bool {
        !matches!(self, Self::Describe { .. })
    }
}

/// Bounded agent description returned by `Describe` — enough for a caller
/// to fence its next command on the current revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct AgentManagementDescription {
    /// Stable lifecycle status label.
    pub status: String,
    /// Current lifecycle revision.
    pub lifecycle_revision: AgentRevisionNumber,
    /// Current definition revision.
    pub definition_revision: AgentRevisionNumber,
    /// Current settings revision.
    pub settings_revision: AgentRevisionNumber,
}

impl From<&AgentEntitySnapshot> for AgentManagementDescription {
    fn from(snapshot: &AgentEntitySnapshot) -> Self {
        Self {
            status: snapshot.status.as_label().to_string(),
            lifecycle_revision: snapshot.lifecycle_revision,
            definition_revision: snapshot.definition_revision,
            settings_revision: snapshot.settings_revision,
        }
    }
}

/// Applied revision surface of an accepted command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct AgentManagementOutcome {
    /// Stable lifecycle status label after the command.
    pub status: String,
    /// Lifecycle revision after the command.
    pub lifecycle_revision: AgentRevisionNumber,
    /// Definition revision after the command.
    pub definition_revision: AgentRevisionNumber,
    /// Settings revision after the command — for an accepted settings
    /// update, the new monotonic revision.
    pub settings_revision: AgentRevisionNumber,
}

impl From<AgentEntityOutcome> for AgentManagementOutcome {
    fn from(outcome: AgentEntityOutcome) -> Self {
        Self {
            status: outcome.status.as_label().to_string(),
            lifecycle_revision: outcome.lifecycle_revision,
            definition_revision: outcome.definition_revision,
            settings_revision: outcome.settings_revision,
        }
    }
}

/// The immediate management response payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "result")]
#[non_exhaustive]
pub enum AgentManagementResponse {
    /// The command transitioned the agent.
    Applied {
        /// Revisions after the transition.
        outcome: AgentManagementOutcome,
    },
    /// The command had already been accepted; this is its original outcome.
    Duplicate {
        /// Revisions of the original transition.
        outcome: AgentManagementOutcome,
    },
    /// The agent's bounded description.
    Described {
        /// Current status and revisions.
        description: AgentManagementDescription,
    },
    /// The entity refused the command — including the stale-revision
    /// conflict a caller rebases on.
    Refused {
        /// Stable domain code, e.g. `stale-settings-revision`.
        code: String,
        /// Bounded refusal message.
        message: String,
    },
}

/// True when the message requests any version of the management extension.
#[must_use]
pub fn is_management_message(message: &Message) -> bool {
    message
        .extensions
        .as_deref()
        .unwrap_or_default()
        .iter()
        .any(|uri| uri.starts_with(AGENT_MANAGEMENT_EXTENSION_PREFIX))
}

/// Parses the management envelope out of a tagged message, failing closed
/// on an unsupported version or a malformed payload.
///
/// # Errors
///
/// Fails closed when the message requests an unsupported management
/// version, carries no single data part, or its envelope does not parse.
pub fn parse_management_request(message: &Message) -> RakkaAgentA2AResult<AgentManagementRequest> {
    let extensions = message.extensions.as_deref().unwrap_or_default();
    let supported = extensions
        .iter()
        .any(|uri| uri == AGENT_MANAGEMENT_EXTENSION_URI);
    if !supported {
        return Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-management",
            reason: "the requested agent-management extension version is not served",
        });
    }
    let mut data_parts = message.parts.iter().filter_map(|part| match &part.content {
        PartContent::Data(value) => Some(value),
        _ => None,
    });
    let Some(payload) = data_parts.next() else {
        return Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-management",
            reason: "a management command must carry exactly one data part",
        });
    };
    if data_parts.next().is_some() {
        return Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-management",
            reason: "a management command must carry exactly one data part",
        });
    }
    let request: AgentManagementRequest =
        serde_json::from_value(payload.clone()).map_err(|_| RakkaAgentA2AError::Unsupported {
            operation: "agent-management",
            reason: "the management envelope does not parse under schema version 1",
        })?;
    if request.schema != AGENT_MANAGEMENT_SCHEMA_VERSION {
        return Err(RakkaAgentA2AError::Unsupported {
            operation: "agent-management",
            reason: "the management envelope names an unsupported schema version",
        });
    }
    Ok(request)
}

/// Builds the immediate response message for one management exchange.
#[must_use]
pub fn management_response_message(
    request_message_id: &str,
    response: &AgentManagementResponse,
) -> Message {
    let payload = serde_json::to_value(response).unwrap_or(Value::Null);
    let mut message = Message::new(
        Role::Agent,
        vec![Part {
            content: PartContent::Data(payload),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.extensions = Some(vec![AGENT_MANAGEMENT_EXTENSION_URI.to_string()]);
    message.message_id = format!("{request_message_id}::management-response");
    message
}

/// Builds the revision provenance an accepted command records: the
/// authenticated principal, the acceptance instant, and the request's
/// causation/audit identity (specification 7.2).
#[must_use]
pub fn management_provenance(
    principal: PrincipalRef,
    message_id: &str,
    audit_ref: Option<&str>,
    now: AgentTimestampMillis,
) -> AgentRevisionProvenance {
    AgentRevisionProvenance {
        principal,
        accepted_at: now,
        causation_id: AgentCausationId::new(message_id.to_string()),
        audit_ref: AgentAuditEventId::new(
            audit_ref
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("a2a:{message_id}")),
        ),
    }
}

/// Maps a domain refusal onto the immediate `Refused` response; an
/// infrastructure failure stays an error.
#[must_use]
pub fn refusal_response(error: &rakka_agent::AgentEntityError) -> Option<AgentManagementResponse> {
    use rakka_agent::AgentEntityError as E;
    match error {
        E::Persistence { .. } | E::Schema(_) => None,
        _ => Some(AgentManagementResponse::Refused {
            code: error.code().to_string(),
            message: error.to_string(),
        }),
    }
}

/// Parses the management response payload out of a response message.
///
/// # Errors
///
/// Fails closed when the message carries no data part or the payload does
/// not parse as a management response.
pub fn parse_management_response(
    message: &Message,
) -> RakkaAgentA2AResult<AgentManagementResponse> {
    let payload = message
        .parts
        .iter()
        .find_map(|part| match &part.content {
            PartContent::Data(value) => Some(value),
            _ => None,
        })
        .ok_or(RakkaAgentA2AError::Unsupported {
            operation: "agent-management",
            reason: "a management response must carry a data part",
        })?;
    serde_json::from_value(payload.clone()).map_err(|_| RakkaAgentA2AError::Unsupported {
        operation: "agent-management",
        reason: "the management response does not parse under schema version 1",
    })
}

/// Builds one management command envelope message, for typed clients and
/// tests.
#[must_use]
pub fn management_request_message(request: &AgentManagementRequest) -> Message {
    let payload = serde_json::to_value(request).unwrap_or(Value::Null);
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(payload),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.extensions = Some(vec![AGENT_MANAGEMENT_EXTENSION_URI.to_string()]);
    message
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn settings_request() -> AgentManagementRequest {
        AgentManagementRequest {
            schema: AGENT_MANAGEMENT_SCHEMA_VERSION,
            command: AgentManagementCommand::UpdateSettings {
                agent: "support".to_string(),
                expected_revision: AgentRevisionNumber::INITIAL,
                changes: vec![AgentSettingsChange::RetrievalLimit(16)],
            },
        }
    }

    #[test]
    fn the_envelope_round_trips_through_a_tagged_message() {
        let request = settings_request();
        let message = management_request_message(&request);
        assert!(is_management_message(&message));
        let parsed = parse_management_request(&message).expect("parse");
        assert_eq!(parsed, request);
        assert!(parsed.command.is_write());
        assert_eq!(parsed.command.agent(), "support");
    }

    #[test]
    fn an_unknown_management_version_fails_closed() {
        let mut message = management_request_message(&settings_request());
        message.extensions = Some(vec![format!("{AGENT_MANAGEMENT_EXTENSION_PREFIX}v999")]);
        assert!(is_management_message(&message));
        assert!(matches!(
            parse_management_request(&message),
            Err(RakkaAgentA2AError::Unsupported { .. })
        ));
    }

    #[test]
    fn an_unknown_schema_version_fails_closed() {
        let mut request = settings_request();
        request.schema = 2;
        let message = management_request_message(&request);
        assert!(matches!(
            parse_management_request(&message),
            Err(RakkaAgentA2AError::Unsupported { .. })
        ));
    }

    #[test]
    fn a_malformed_envelope_fails_closed() {
        let mut message = management_request_message(&settings_request());
        message.parts = vec![Part {
            content: PartContent::Data(json!({"not": "an envelope"})),
            filename: None,
            media_type: None,
            metadata: None,
        }];
        assert!(matches!(
            parse_management_request(&message),
            Err(RakkaAgentA2AError::Unsupported { .. })
        ));
    }

    #[test]
    fn an_untagged_message_is_not_management() {
        let message = Message::new(Role::User, vec![Part::text("hello")]);
        assert!(!is_management_message(&message));
    }

    #[test]
    fn the_response_message_echoes_the_extension_and_the_payload() {
        let response = AgentManagementResponse::Refused {
            code: "stale-settings-revision".to_string(),
            message: "expected 1, current 3".to_string(),
        };
        let message = management_response_message("msg-9", &response);
        assert!(is_management_message(&message));
        assert_eq!(message.message_id, "msg-9::management-response");
        let PartContent::Data(payload) = &message.parts[0].content else {
            panic!("expected a data part");
        };
        assert_eq!(
            payload.get("result").and_then(Value::as_str),
            Some("refused")
        );
    }
}
