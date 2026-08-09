//! The A2A-backed [`AgentClientTransport`] implementation.
//!
//! `rakka-agent` defines [`rakka_agent::RakkaAgentClient`] over a durable
//! command port; this module is the port's transport. Every call encodes
//! onto the same [`RakkaAgentA2AService`] operations an external A2A caller
//! uses — the same normalization, authorization, durable deduplicated
//! acceptance, and projection — so a client call and a network call are
//! indistinguishable to the entities. There is no local actor shortcut
//! (specification 14.5).

use std::sync::Arc;

use a2a::{
    CancelTaskRequest, Message, Part, PartContent, Role, SendMessageRequest, Task, TaskState,
};
use a2a_server::ServiceParams;
use rakka_agent::{
    AgentClientAgentStatus, AgentClientError, AgentClientFuture, AgentClientManagementCommand,
    AgentClientManagementResponse, AgentClientTaskEvent, AgentClientTaskRequest,
    AgentClientTaskState, AgentClientTaskView, AgentClientTransport, AgentEntityState,
    AgentRunState, AgentTaskHistoryStore, AgentTaskId, AgentTaskState,
};
use rakka_agent_workflow::PrincipalRef;
use rakka_persistence::DurableStateStore;
use serde_json::{Map, Value};

use crate::mapping::{META_DEDUPLICATION_KEY, META_PRINCIPAL_REF};
use crate::task::TaskProjectionError;

use super::error::RakkaAgentA2AError;
use super::ingress::{META_AGENT_ID, META_TASK_DEFINITION};
use super::management::{
    management_request_message, parse_management_response, AgentManagementCommand,
    AgentManagementRequest, AgentManagementResponse, AGENT_MANAGEMENT_SCHEMA_VERSION,
};
use super::service::RakkaAgentA2AService;

/// A2A-backed transport for [`rakka_agent::RakkaAgentClient`].
///
/// Wraps the agents-surface service core with a fixed caller identity: the
/// service params, tenant, and default principal every call carries.
pub struct A2AAgentClientTransport<Tasks, Agents, History, Runs, Teams, TeamHistory>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
    Teams: DurableStateStore<rakka_agent::AgentTeamState>,
    TeamHistory: rakka_agent::AgentTeamHistoryStore + Clone,
{
    service: Arc<RakkaAgentA2AService<Tasks, Agents, History, Runs, Teams, TeamHistory>>,
    tenant: Option<String>,
    principal: Option<PrincipalRef>,
}

impl<Tasks, Agents, History, Runs, Teams, TeamHistory>
    A2AAgentClientTransport<Tasks, Agents, History, Runs, Teams, TeamHistory>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
    Teams: DurableStateStore<rakka_agent::AgentTeamState>,
    TeamHistory: rakka_agent::AgentTeamHistoryStore + Clone,
{
    /// Wraps a service.
    #[must_use]
    pub const fn new(
        service: Arc<RakkaAgentA2AService<Tasks, Agents, History, Runs, Teams, TeamHistory>>,
    ) -> Self {
        Self {
            service,
            tenant: None,
            principal: None,
        }
    }

    /// Sets the tenant every call resolves under.
    #[must_use]
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Sets the default authenticated principal for calls that carry none.
    #[must_use]
    pub fn with_principal(mut self, principal: PrincipalRef) -> Self {
        self.principal = Some(principal);
        self
    }

    fn params() -> ServiceParams {
        ServiceParams::new()
    }

    fn principal_metadata(principal: &PrincipalRef) -> Value {
        let mut encoded = format!("{}:{}", principal.principal_type, principal.principal_id);
        if let Some(display) = &principal.display_name {
            encoded.push(':');
            encoded.push_str(display);
        }
        Value::String(encoded)
    }
}

fn client_state(state: &TaskState) -> AgentClientTaskState {
    match state {
        TaskState::Submitted => AgentClientTaskState::Submitted,
        TaskState::Working => AgentClientTaskState::Working,
        TaskState::InputRequired => AgentClientTaskState::InputRequired,
        TaskState::AuthRequired => AgentClientTaskState::AuthRequired,
        TaskState::Completed => AgentClientTaskState::Completed,
        TaskState::Failed => AgentClientTaskState::Failed,
        TaskState::Canceled => AgentClientTaskState::Canceled,
        TaskState::Rejected => AgentClientTaskState::Rejected,
        TaskState::Unspecified => AgentClientTaskState::Unknown,
    }
}

fn client_view(task: &Task) -> Result<AgentClientTaskView, AgentClientError> {
    let id = AgentTaskId::new(&task.id).map_err(|error| AgentClientError::Transport {
        code: "invalid-identity".to_string(),
        message: error.to_string(),
    })?;
    let metadata: Map<String, Value> = task
        .metadata
        .as_ref()
        .map(|metadata| metadata.clone().into_iter().collect())
        .unwrap_or_default();
    Ok(AgentClientTaskView {
        task: id,
        context: task.context_id.clone(),
        state: client_state(&task.status.state),
        metadata,
    })
}

fn client_error(error: RakkaAgentA2AError) -> AgentClientError {
    match error {
        RakkaAgentA2AError::TaskNotFound { task_id } => {
            AgentClientError::TaskNotFound { task: task_id }
        }
        RakkaAgentA2AError::Projection(TaskProjectionError::TaskNotFound { task_id }) => {
            AgentClientError::TaskNotFound { task: task_id }
        }
        RakkaAgentA2AError::Projection(TaskProjectionError::ReplayWindowExpired { .. }) => {
            AgentClientError::ReplayWindowExpired
        }
        RakkaAgentA2AError::Refused { code, message } => {
            AgentClientError::Refused { code, message }
        }
        other => AgentClientError::Transport {
            code: other.code().to_string(),
            message: other.to_string(),
        },
    }
}

fn client_status(status: &super::management::AgentManagementDescription) -> AgentClientAgentStatus {
    AgentClientAgentStatus {
        status: status.status.clone(),
        lifecycle_revision: status.lifecycle_revision,
        definition_revision: status.definition_revision,
        settings_revision: status.settings_revision,
    }
}

fn client_outcome(outcome: &super::management::AgentManagementOutcome) -> AgentClientAgentStatus {
    AgentClientAgentStatus {
        status: outcome.status.clone(),
        lifecycle_revision: outcome.lifecycle_revision,
        definition_revision: outcome.definition_revision,
        settings_revision: outcome.settings_revision,
    }
}

fn management_command(
    command: AgentClientManagementCommand,
) -> Result<AgentManagementCommand, AgentClientError> {
    Ok(match command {
        AgentClientManagementCommand::UpdateSettings {
            agent,
            expected_revision,
            changes,
        } => AgentManagementCommand::UpdateSettings {
            agent,
            expected_revision,
            changes,
        },
        AgentClientManagementCommand::Suspend {
            agent,
            expected_lifecycle_revision,
        } => AgentManagementCommand::Suspend {
            agent,
            expected_lifecycle_revision,
        },
        AgentClientManagementCommand::Resume {
            agent,
            expected_lifecycle_revision,
        } => AgentManagementCommand::Resume {
            agent,
            expected_lifecycle_revision,
        },
        AgentClientManagementCommand::Terminate {
            agent,
            expected_lifecycle_revision,
        } => AgentManagementCommand::Terminate {
            agent,
            expected_lifecycle_revision,
        },
        AgentClientManagementCommand::Describe { agent } => {
            AgentManagementCommand::Describe { agent }
        }
        other => {
            return Err(AgentClientError::Transport {
                code: "unsupported-command".to_string(),
                message: format!("this transport does not encode {other:?}"),
            });
        }
    })
}

impl<Tasks, Agents, History, Runs, Teams, TeamHistory> AgentClientTransport
    for A2AAgentClientTransport<Tasks, Agents, History, Runs, Teams, TeamHistory>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
    Teams: DurableStateStore<rakka_agent::AgentTeamState>,
    TeamHistory: rakka_agent::AgentTeamHistoryStore + Clone,
{
    fn create_task(
        &self,
        request: AgentClientTaskRequest,
    ) -> AgentClientFuture<'_, AgentClientTaskView> {
        Box::pin(async move {
            let mut message = Message::new(
                Role::User,
                vec![Part {
                    content: PartContent::Data(request.input),
                    filename: None,
                    media_type: Some("application/json".to_string()),
                    metadata: None,
                }],
            );
            message.context_id = request.context;
            let mut metadata = serde_json::Map::new();
            if let Some(key) = request.deduplication_key {
                metadata.insert(META_DEDUPLICATION_KEY.to_string(), Value::String(key));
            }
            if let Some(agent) = request.agent {
                metadata.insert(META_AGENT_ID.to_string(), Value::String(agent));
            }
            if let Some(definition) = request.task_definition {
                metadata.insert(META_TASK_DEFINITION.to_string(), Value::String(definition));
            }
            if let Some(principal) = request.principal.as_ref().or(self.principal.as_ref()) {
                metadata.insert(
                    META_PRINCIPAL_REF.to_string(),
                    Self::principal_metadata(principal),
                );
            }
            // Egress injection (specification 17.5): the caller's context
            // rides the standard W3C metadata keys, and the ingress extraction
            // on the receiving side is its mirror. Invalid context injects
            // nothing rather than failing the send — telemetry is never a
            // correctness input.
            if let Some(telemetry) = request.telemetry.as_ref() {
                let mut carrier = rakka_agent_workflow::AgentAttributes::new();
                if rakka_agent_workflow::inject_agent_trace_context(telemetry, &mut carrier).is_ok()
                {
                    for (key, value) in carrier {
                        metadata.insert(key, Value::String(value));
                    }
                }
            }
            let send = SendMessageRequest {
                message,
                configuration: None,
                metadata: Some(metadata.into_iter().collect()),
                tenant: self.tenant.clone(),
            };
            let task = self
                .service
                .send_message(&Self::params(), &send)
                .await
                .map_err(client_error)?;
            client_view(&task)
        })
    }

    fn task<'a>(&'a self, task: &'a str) -> AgentClientFuture<'a, Option<AgentClientTaskView>> {
        Box::pin(async move {
            match self
                .service
                .get_task(&Self::params(), self.tenant.as_deref(), task, None)
                .await
            {
                Ok(task) => client_view(&task).map(Some),
                Err(error) => match client_error(error) {
                    AgentClientError::TaskNotFound { .. } => Ok(None),
                    error => Err(error),
                },
            }
        })
    }

    fn cancel_task<'a>(&'a self, task: &'a str) -> AgentClientFuture<'a, AgentClientTaskView> {
        Box::pin(async move {
            let request = CancelTaskRequest {
                id: task.to_string(),
                metadata: None,
                tenant: self.tenant.clone(),
            };
            let task = self
                .service
                .cancel_task(&Self::params(), &request)
                .await
                .map_err(client_error)?;
            client_view(&task)
        })
    }

    fn manage(
        &self,
        command: AgentClientManagementCommand,
        principal: Option<PrincipalRef>,
    ) -> AgentClientFuture<'_, AgentClientManagementResponse> {
        Box::pin(async move {
            let request = AgentManagementRequest {
                schema: AGENT_MANAGEMENT_SCHEMA_VERSION,
                command: management_command(command)?,
            };
            let message = management_request_message(&request);
            let mut metadata = serde_json::Map::new();
            if let Some(principal) = principal.as_ref().or(self.principal.as_ref()) {
                metadata.insert(
                    META_PRINCIPAL_REF.to_string(),
                    Self::principal_metadata(principal),
                );
            }
            let send = SendMessageRequest {
                message,
                configuration: None,
                metadata: Some(metadata.into_iter().collect()),
                tenant: self.tenant.clone(),
            };
            let response = self
                .service
                .manage_agent(&Self::params(), &send)
                .await
                .map_err(client_error)?;
            let response = parse_management_response(&response).map_err(client_error)?;
            Ok(match response {
                AgentManagementResponse::Applied { outcome } => {
                    AgentClientManagementResponse::Applied(client_outcome(&outcome))
                }
                AgentManagementResponse::Duplicate { outcome } => {
                    AgentClientManagementResponse::Duplicate(client_outcome(&outcome))
                }
                AgentManagementResponse::Described { description } => {
                    AgentClientManagementResponse::Described(client_status(&description))
                }
                AgentManagementResponse::Refused { code, message } => {
                    AgentClientManagementResponse::Refused { code, message }
                }
            })
        })
    }

    fn task_events<'a>(
        &'a self,
        task: &'a str,
        after_cursor: Option<&'a str>,
    ) -> AgentClientFuture<'a, Vec<AgentClientTaskEvent>> {
        Box::pin(async move {
            let events = self
                .service
                .replay_task_events(&Self::params(), self.tenant.as_deref(), task, after_cursor)
                .await
                .map_err(client_error)?;
            Ok(events
                .iter()
                .map(|event| AgentClientTaskEvent {
                    sequence: event.sequence,
                    cursor: event.replay_cursor(),
                    kind: event.kind().as_label().to_string(),
                    state: match &event.projected_state {
                        TaskState::Unspecified => None,
                        state => Some(client_state(state)),
                    },
                    occurred_at: event.occurred_at,
                })
                .collect())
        })
    }
}
