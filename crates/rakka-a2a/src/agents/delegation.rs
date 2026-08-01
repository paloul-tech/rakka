//! The in-process delegation-send executor.
//!
//! Implements [`rakka_agent::AgentA2aSendExecutor`] over the agents-surface
//! service core: a parent run's outbound A2A send enters the exact
//! normalization, authorization, catalog resolution, durable deduplicated
//! acceptance, and projection path an external A2A caller uses
//! (specification 14.4). There is no local entity shortcut — the executor is
//! to the delegation effect what [`super::client::A2AAgentClientTransport`]
//! is to the typed client.
//!
//! The send carries the persisted [`rakka_agent::AgentDelegationRecord`]
//! verbatim: its delegation id as the message id, its deduplication key as
//! the `io.rakka.command.deduplication_key`, its resolved target as the
//! `io.rakka.agent.*` selection, and its collaboration envelope under
//! [`super::collaboration::META_COLLABORATION`] with the v1 extension URI
//! declared. Because the receiving surface derives the child task id from
//! the deduplication key, every retry of one delegation converges on one
//! logical child — and a child that answers under a *different* delegation
//! identity is reported as the explicit conflict of specification 6.6, never
//! adopted.

use std::sync::Arc;

use a2a::{Message, Part, PartContent, Role, SendMessageRequest};
use a2a_server::ServiceParams;
use rakka_agent::{
    AgentA2aSendExecutor, AgentA2aSendFinding, AgentDelegationRecord, AgentDispatchError,
    AgentDispatchFuture, AgentEntityState, AgentRunEffect, AgentRunScope, AgentRunState,
    AgentTaskError, AgentTaskHistoryStore, AgentTaskId, AgentTaskState,
};
use rakka_agent_workflow::PrincipalRef;
use rakka_persistence::DurableStateStore;
use serde_json::{Map, Value};

use crate::mapping::{META_DEDUPLICATION_KEY, META_PRINCIPAL_REF};

use super::collaboration::{
    AgentCollaborationMetadata, AGENT_COLLABORATION_EXTENSION_URI, META_COLLABORATION,
};
use super::error::RakkaAgentA2AError;
use super::ingress::{META_AGENT_ID, META_TASK_DEFINITION};
use super::service::RakkaAgentA2AService;

/// In-process [`AgentA2aSendExecutor`] over the agents-surface service core.
pub struct A2AAgentDelegationSendExecutor<Tasks, Agents, History, Runs>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
{
    service: Arc<RakkaAgentA2AService<Tasks, Agents, History, Runs>>,
    principal: Option<PrincipalRef>,
}

impl<Tasks, Agents, History, Runs> A2AAgentDelegationSendExecutor<Tasks, Agents, History, Runs>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
{
    /// Wraps a service.
    #[must_use]
    pub const fn new(service: Arc<RakkaAgentA2AService<Tasks, Agents, History, Runs>>) -> Self {
        Self {
            service,
            principal: None,
        }
    }

    /// Sets the principal recorded as the delegating caller's identity.
    #[must_use]
    pub fn with_principal(mut self, principal: PrincipalRef) -> Self {
        self.principal = Some(principal);
        self
    }

    fn request_for(&self, record: &AgentDelegationRecord) -> Result<SendMessageRequest, String> {
        // The parent-side interception only ever builds bounded inline
        // input; anything else means a record this executor cannot encode
        // as message parts, refused rather than half-sent.
        let Some(input) = record.input.inline_value() else {
            return Err("the delegation input is not inline content".to_string());
        };
        let mut message = Message::new(
            Role::User,
            vec![Part {
                content: PartContent::Data(input.clone()),
                filename: None,
                media_type: Some("application/json".to_string()),
                metadata: None,
            }],
        );
        message.message_id = record.a2a_message_id.clone();
        message.extensions = Some(vec![AGENT_COLLABORATION_EXTENSION_URI.to_string()]);

        let mut metadata = Map::new();
        metadata.insert(
            META_DEDUPLICATION_KEY.to_string(),
            Value::String(record.deduplication_key.clone()),
        );
        metadata.insert(
            META_AGENT_ID.to_string(),
            Value::String(record.resolved.agent.as_str().to_string()),
        );
        metadata.insert(
            META_TASK_DEFINITION.to_string(),
            Value::String(record.resolved.task_definition.as_str().to_string()),
        );
        metadata.insert(
            META_COLLABORATION.to_string(),
            AgentCollaborationMetadata::from_record(record).to_value(),
        );
        if let Some(principal) = self.principal.as_ref() {
            let mut encoded = format!("{}:{}", principal.principal_type, principal.principal_id);
            if let Some(display) = &principal.display_name {
                encoded.push(':');
                encoded.push_str(display);
            }
            metadata.insert(META_PRINCIPAL_REF.to_string(), Value::String(encoded));
        }
        // Egress injection (specification 17.5): the record's committed
        // context rides the standard W3C keys; invalid context injects
        // nothing rather than failing the send.
        let mut carrier = rakka_agent_workflow::AgentAttributes::new();
        if rakka_agent_workflow::inject_agent_trace_context(&record.telemetry, &mut carrier).is_ok()
        {
            for (key, value) in carrier {
                metadata.insert(key, Value::String(value));
            }
        }
        Ok(SendMessageRequest {
            message,
            configuration: None,
            metadata: Some(metadata.into_iter().collect()),
            tenant: Some(record.parent_run.tenant().as_str().to_string()),
        })
    }
}

/// Maps one service outcome onto the executor's finding vocabulary.
///
/// Definitive answers — a conflict, a version refusal, an authorization or
/// normalization failure — become findings; store and read failures become
/// retryable attempt errors under the effect's idempotent attempt bound,
/// which the derived deduplication key makes safe.
fn finding_for_error(error: RakkaAgentA2AError) -> Result<AgentA2aSendFinding, AgentDispatchError> {
    match error {
        RakkaAgentA2AError::Task(AgentTaskError::AlreadyCreated { .. }) => {
            Ok(AgentA2aSendFinding::Conflict {
                code: "delegation-child-conflict".to_string(),
                message: "the peer holds an already-created task this delegation's identity does \
                          not own"
                    .to_string(),
            })
        }
        RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason,
        } => Ok(AgentA2aSendFinding::Refused {
            code: "collaboration-version-unsupported".to_string(),
            message: reason.to_string(),
        }),
        RakkaAgentA2AError::Task(error) => match &error {
            AgentTaskError::Persistence(_) => Err(AgentDispatchError::Invocation {
                code: error.code(),
                message: error.to_string(),
            }),
            _ => Ok(AgentA2aSendFinding::Refused {
                code: error.code().to_string(),
                message: error.to_string(),
            }),
        },
        RakkaAgentA2AError::Entity(_)
        | RakkaAgentA2AError::Run(_)
        | RakkaAgentA2AError::Projection(_) => Err(AgentDispatchError::Invocation {
            code: error.code(),
            message: error.to_string(),
        }),
        definitive => Ok(AgentA2aSendFinding::Refused {
            code: definitive.code().to_string(),
            message: definitive.to_string(),
        }),
    }
}

impl<Tasks, Agents, History, Runs> AgentA2aSendExecutor
    for A2AAgentDelegationSendExecutor<Tasks, Agents, History, Runs>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
{
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        delegation: &'a AgentDelegationRecord,
        _credential: Option<&'a rakka_agent_workflow::AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aSendFinding> {
        Box::pin(async move {
            let send = match self.request_for(delegation) {
                Ok(send) => send,
                Err(message) => {
                    return Ok(AgentA2aSendFinding::Refused {
                        code: "delegation-input-unsupported".to_string(),
                        message,
                    });
                }
            };
            let task = match self
                .service
                .send_message(&ServiceParams::new(), &send)
                .await
            {
                Ok(task) => task,
                Err(error) => return finding_for_error(error),
            };
            // The identity check behind the deduplication key: the answering
            // task must echo *this* delegation. A task that answers without
            // the echo, or under another delegation, is a child this
            // delegation does not own — the explicit conflict, never an
            // adoption.
            let echoed = task
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get(META_COLLABORATION))
                .and_then(|echo| echo.get("delegation"))
                .and_then(Value::as_str);
            if echoed != Some(delegation.delegation.as_str()) {
                return Ok(AgentA2aSendFinding::Conflict {
                    code: "delegation-child-mismatch".to_string(),
                    message: format!(
                        "the answering task {} does not echo delegation {}",
                        task.id, delegation.delegation
                    ),
                });
            }
            let child_task =
                AgentTaskId::new(&task.id).map_err(|error| AgentDispatchError::Invocation {
                    code: "invalid-identity",
                    message: error.to_string(),
                })?;
            let peer_status = serde_json::to_value(task.status.state)
                .ok()
                .and_then(|value| value.as_str().map(ToString::to_string))
                .unwrap_or_else(|| "unknown".to_string());
            Ok(AgentA2aSendFinding::Sent {
                child_task,
                child_run: None,
                peer_status,
            })
        })
    }
}
