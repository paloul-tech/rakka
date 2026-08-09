//! The in-process handoff-send executor.
//!
//! Implements [`rakka_agent::AgentA2aHandoffSendExecutor`] over the
//! agents-surface service core: a source run's outbound handoff enters the
//! exact normalization, authorization, durable deduplicated acceptance, and
//! projection path an external A2A caller uses (specification 8.9 and 14.4).
//! There is no local entity shortcut, including when source and target are
//! colocated.
//!
//! The send carries the persisted [`rakka_agent::AgentHandoffRecord`]
//! verbatim: its handoff id as the message id, its deduplication key as the
//! `io.rakka.command.deduplication_key`, the *same* `AgentTaskId` as
//! `message.task_id` — a handoff continues a task, never creates one — and
//! its handoff cluster under [`super::collaboration::META_COLLABORATION`]
//! with the v1 extension URI declared.
//!
//! On an ambiguous failure the executor probes the task's authoritative
//! durable state before giving up: a task whose materialized handoff
//! provenance names this handoff proves the transfer was durably recorded —
//! the finding is `Recorded`, converged exactly as an in-window replay would
//! have been — while an *unresolved* transfer under a different identity is
//! the explicit conflict. The probe never answers definitively in the
//! negative: an absent record at read time cannot prove the ambiguously
//! failed write will never land, so that case — like a probe that cannot
//! answer at all — leaves the attempt as a retryable error, and the
//! deduplicated re-send converges on the recorded transfer or records it
//! fresh. When the attempt budget spends out, the source run parks
//! indeterminate rather than resuming beside a possibly-live transfer.

use std::sync::Arc;

use a2a::{Message, Part, PartContent, Role, SendMessageRequest, Task, TaskState};
use a2a_server::ServiceParams;
use rakka_agent::{
    AgentA2aHandoffFinding, AgentA2aHandoffSendExecutor, AgentAssignmentGeneration,
    AgentDispatchError, AgentDispatchFuture, AgentEntityState, AgentHandoffRecord, AgentRunEffect,
    AgentRunScope, AgentRunState, AgentTaskError, AgentTaskHistoryStore, AgentTaskState,
};
use rakka_agent_workflow::PrincipalRef;
use rakka_persistence::DurableStateStore;
use serde_json::{Map, Value};

use crate::mapping::{META_DEDUPLICATION_KEY, META_PRINCIPAL_REF};

use super::collaboration::{
    AgentHandoffCollaborationMetadata, AGENT_COLLABORATION_EXTENSION_URI, META_COLLABORATION,
};
use super::error::RakkaAgentA2AError;
use super::ingress::{META_AGENT_ID, META_TASK_DEFINITION};
use super::projection::{agent_task_state, AgentTaskCondition};
use super::service::RakkaAgentA2AService;

/// In-process [`AgentA2aHandoffSendExecutor`] over the agents-surface
/// service core.
pub struct A2AAgentHandoffSendExecutor<Tasks, Agents, History, Runs, Teams, TeamHistory>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
    Teams: DurableStateStore<rakka_agent::AgentTeamState>,
    TeamHistory: rakka_agent::AgentTeamHistoryStore + Clone,
{
    service: Arc<RakkaAgentA2AService<Tasks, Agents, History, Runs, Teams, TeamHistory>>,
    principal: Option<PrincipalRef>,
}

impl<Tasks, Agents, History, Runs, Teams, TeamHistory>
    A2AAgentHandoffSendExecutor<Tasks, Agents, History, Runs, Teams, TeamHistory>
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
            principal: None,
        }
    }

    /// Sets the principal recorded as the handing-off caller's identity.
    #[must_use]
    pub fn with_principal(mut self, principal: PrincipalRef) -> Self {
        self.principal = Some(principal);
        self
    }

    fn request_for(&self, record: &AgentHandoffRecord) -> SendMessageRequest {
        let mut message = Message::new(
            Role::User,
            vec![Part {
                content: PartContent::Data(serde_json::json!({
                    "handoff": record.handoff.as_str(),
                })),
                filename: None,
                media_type: Some("application/json".to_string()),
                metadata: None,
            }],
        );
        message.message_id = record.a2a_message_id.clone();
        // The same task, continued: this is what routes the send into the
        // handoff arm rather than a creation.
        message.task_id = Some(record.task.as_str().to_string());
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
            AgentHandoffCollaborationMetadata::from_record(record).to_value(),
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
        SendMessageRequest {
            message,
            configuration: None,
            metadata: Some(metadata.into_iter().collect()),
            tenant: Some(record.source_run.tenant().as_str().to_string()),
        }
    }

    /// Probes the task's authoritative durable state for this handoff.
    ///
    /// Reads the durable task snapshot directly — never the public
    /// projection, which is an observability read model that may lag the
    /// commit, and never `tasks/get`, whose deployment authorization gates
    /// external callers while this probe is the deployment's own recovery
    /// step. `Ok(Some(finding))` is a definitive answer; `Ok(None)` means
    /// the task's durable state records no transfer under this handoff's
    /// identity *at read time* — a task carrying no handoff, or only a
    /// settled previous hop. That is deliberately not definitive: the write
    /// that failed ambiguously may still land after this read, so the
    /// caller keeps the attempt retryable rather than resuming the source;
    /// `Err` means the probe itself could not answer.
    async fn probe(
        &self,
        record: &AgentHandoffRecord,
    ) -> Result<Option<AgentA2aHandoffFinding>, RakkaAgentA2AError> {
        let Some((snapshot, run)) = self
            .service
            .authoritative_task_view(record.source_run.tenant().as_str(), record.task.as_str())
            .await?
        else {
            // No durable task at all: nothing can have recorded the transfer.
            return Ok(None);
        };
        let peer_status = peer_status_label(&agent_task_state(AgentTaskCondition {
            task: snapshot.status,
            run,
        }));
        match snapshot.handoff.as_deref() {
            Some(held) if held.handoff == record.handoff => {
                Ok(Some(AgentA2aHandoffFinding::Recorded {
                    target_generation: held.target_generation,
                    peer_status: peer_status.to_string(),
                }))
            }
            Some(held) if !held.is_settled() => Ok(Some(AgentA2aHandoffFinding::Conflict {
                code: "handoff-conflict".to_string(),
                message: format!(
                    "the task {} carries the unresolved transfer {}, which this handoff's \
                     identity does not own",
                    record.task, held.handoff
                ),
            })),
            // No transfer, or a previous hop that already settled: history,
            // not a conflict — this handoff was provably never recorded.
            _ => Ok(None),
        }
    }
}

/// The stable kebab-case label of one peer task state, for the handoff
/// receipt's durable `peer_status`.
fn peer_status_label(state: &TaskState) -> &'static str {
    match state {
        TaskState::Unspecified => "unspecified",
        TaskState::Submitted => "submitted",
        TaskState::Working => "working",
        TaskState::Completed => "completed",
        TaskState::Failed => "failed",
        TaskState::Canceled => "canceled",
        TaskState::InputRequired => "input-required",
        TaskState::Rejected => "rejected",
        TaskState::AuthRequired => "auth-required",
    }
}

/// The handoff id the task's collaboration echo names, when it names one.
///
/// The echo is recorded when the transfer durably commits, so it answers the
/// ownership question even after the handoff operation aged out of the
/// task's bounded deduplication window.
fn handoff_echo_id(task: &Task) -> Option<&str> {
    task.metadata
        .as_ref()
        .and_then(|metadata| metadata.get(META_COLLABORATION))
        .and_then(|echo| echo.get("handoff"))
        .and_then(Value::as_str)
}

/// Maps one service outcome onto the executor's finding vocabulary.
///
/// Definitive answers — a version refusal, a non-committing handoff refusal,
/// an authorization or normalization failure — become findings; store and
/// read failures become retryable attempt errors under the effect's
/// idempotent attempt bound, which the derived deduplication key makes safe.
fn finding_for_error(
    error: RakkaAgentA2AError,
) -> Result<AgentA2aHandoffFinding, AgentDispatchError> {
    match error {
        RakkaAgentA2AError::Unsupported {
            operation: "agent-collaboration",
            reason,
        } => Ok(AgentA2aHandoffFinding::Refused {
            code: "collaboration-version-unsupported".to_string(),
            message: reason.to_string(),
        }),
        RakkaAgentA2AError::Task(error) => {
            if task_error_is_ambiguous(&error) {
                Err(AgentDispatchError::Invocation {
                    code: error.code(),
                    message: error.to_string(),
                })
            } else {
                Ok(AgentA2aHandoffFinding::Refused {
                    code: error.code().to_string(),
                    message: error.to_string(),
                })
            }
        }
        RakkaAgentA2AError::Refused { code, message } => {
            Ok(AgentA2aHandoffFinding::Refused { code, message })
        }
        RakkaAgentA2AError::Entity(_)
        | RakkaAgentA2AError::Run(_)
        | RakkaAgentA2AError::Projection(_) => Err(AgentDispatchError::Invocation {
            code: error.code(),
            message: error.to_string(),
        }),
        definitive => Ok(AgentA2aHandoffFinding::Refused {
            code: definitive.code().to_string(),
            message: definitive.to_string(),
        }),
    }
}

impl<Tasks, Agents, History, Runs, Teams, TeamHistory> AgentA2aHandoffSendExecutor
    for A2AAgentHandoffSendExecutor<Tasks, Agents, History, Runs, Teams, TeamHistory>
where
    Tasks: DurableStateStore<AgentTaskState>,
    Agents: DurableStateStore<AgentEntityState>,
    History: AgentTaskHistoryStore + Clone,
    Runs: DurableStateStore<AgentRunState>,
    Teams: DurableStateStore<rakka_agent::AgentTeamState>,
    TeamHistory: rakka_agent::AgentTeamHistoryStore + Clone,
{
    fn execute<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _intent: &'a AgentRunEffect,
        handoff: &'a AgentHandoffRecord,
        _credential: Option<&'a rakka_agent_workflow::AgentEphemeralCredential>,
    ) -> AgentDispatchFuture<'a, AgentA2aHandoffFinding> {
        Box::pin(async move {
            let send = self.request_for(handoff);
            let task = match self
                .service
                .send_message(&ServiceParams::new(), &send)
                .await
            {
                Ok(task) => task,
                Err(error) if error_is_retryable(&error) => {
                    // The ambiguous case: the send may or may not have
                    // committed. The probe answers definitively only in the
                    // affirmative — a recorded transfer, or a foreign
                    // unresolved one. An *absent* record is not proof of
                    // absence: the very write that failed ambiguously may
                    // still land after the probe reads, so declaring the
                    // transfer definitively unrecorded would resume the
                    // source beside a live transfer. The attempt stays
                    // retryable instead: the deduplicated re-send converges
                    // — echoing the late-landing write or recording fresh —
                    // and an exhausted budget parks the source indeterminate
                    // rather than resuming it.
                    return match self.probe(handoff).await {
                        Ok(Some(finding)) => Ok(finding),
                        Ok(None) => Err(AgentDispatchError::Invocation {
                            code: "handoff-unrecorded",
                            message: "the send failed and the task's durable state does not \
                                      record the transfer yet; absence at probe time cannot \
                                      prove the failed write will never land, so the \
                                      deduplicated send re-drives"
                                .to_string(),
                        }),
                        Err(probe_error) => Err(AgentDispatchError::Invocation {
                            code: probe_error.code(),
                            message: probe_error.to_string(),
                        }),
                    };
                }
                Err(error) => return finding_for_error(error),
            };
            // The identity check behind the deduplication key: the answering
            // task must echo *this* handoff.
            match handoff_echo_id(&task) {
                Some(echoed) if echoed == handoff.handoff.as_str() => {}
                Some(_) => {
                    return Ok(AgentA2aHandoffFinding::Conflict {
                        code: "handoff-conflict".to_string(),
                        message: format!(
                            "the answering task {} echoes a transfer this handoff's identity \
                             does not own",
                            task.id
                        ),
                    });
                }
                None => {
                    return Ok(AgentA2aHandoffFinding::Conflict {
                        code: "handoff-not-echoed".to_string(),
                        message: format!(
                            "the answering task {} does not echo handoff {}",
                            task.id, handoff.handoff
                        ),
                    });
                }
            }
            let target_generation = task
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get(META_COLLABORATION))
                .and_then(|echo| echo.get("handoff-target-generation"))
                .and_then(Value::as_u64)
                .map(AgentAssignmentGeneration::new);
            Ok(AgentA2aHandoffFinding::Recorded {
                target_generation,
                peer_status: peer_status_label(&task.status.state).to_string(),
            })
        })
    }
}

/// Whether a task-entity error leaves the send's outcome unknown: a store
/// failure that may have struck around the durable commit, whichever layer
/// wrapped it — the entity facade surfaces a write failure through the
/// choreography host, not only as a bare persistence error.
fn task_error_is_ambiguous(error: &AgentTaskError) -> bool {
    match error {
        AgentTaskError::Persistence(_) => true,
        AgentTaskError::Choreography(inner) => matches!(
            inner.as_ref(),
            rakka_agent::AgentChoreographyError::Persistence(_)
        ),
        _ => false,
    }
}

/// Whether a service error leaves the send's outcome unknown: a store or
/// read failure that may have struck after the durable commit.
fn error_is_retryable(error: &RakkaAgentA2AError) -> bool {
    match error {
        RakkaAgentA2AError::Task(error) => task_error_is_ambiguous(error),
        RakkaAgentA2AError::Entity(_)
        | RakkaAgentA2AError::Run(_)
        | RakkaAgentA2AError::Projection(_) => true,
        _ => false,
    }
}
