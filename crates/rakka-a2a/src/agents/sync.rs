//! Agent task-to-projection synchronization.
//!
//! The agents-surface analog of the substrate's run synchronization: after a
//! durably accepted command (and after any read that finds the public view
//! behind), the authoritative task snapshot plus the current run condition
//! are projected into the shared [`A2ATaskProjectionStore`], producing the
//! same replayable public event stream the substrate surface emits. The
//! projection remains a public view; durable task/run state stays the
//! correctness source.

use a2a::Message;
use rakka_agent::{AgentRunStatus, AgentTaskSnapshot};
use rakka_agent_workflow::AgentTimestampMillis;
use serde_json::Value;
use std::collections::HashMap;

use crate::projection::A2ATaskProjectionStore;
use crate::task::{
    status_transition_allowed, A2ATaskEvent, A2ATaskEventPayload, A2ATaskProjection,
    TaskProjectionError, META_PROJECTION_REVISION, META_STATUS_SOURCE,
};

use super::error::RakkaAgentA2AResult;
use super::ingress::META_AGENT_ID;
use super::projection::{agent_task_state, agent_task_state_metadata, AgentTaskCondition};

/// Status-source label recorded on projections assembled from durable agent
/// task snapshots.
pub const AGENT_STATUS_SOURCE: &str = "rakka-agent-task";

/// Metadata key carrying the typed task-definition id on agent projections.
pub const META_AGENT_TASK_DEFINITION_ID: &str = "io.rakka.agent.task-definition";

/// Assembles the public projection record for one authoritative snapshot.
#[must_use]
pub(crate) fn agent_projection_from_snapshot(
    snapshot: &AgentTaskSnapshot,
    run: Option<AgentRunStatus>,
    tenant: &str,
    context_id: &str,
    history: Vec<Message>,
    projection_revision: u64,
) -> A2ATaskProjection {
    let condition = AgentTaskCondition {
        task: snapshot.status,
        run,
    };
    let mut metadata: HashMap<String, Value> = agent_task_state_metadata(condition);
    metadata.insert(
        META_AGENT_TASK_DEFINITION_ID.to_string(),
        Value::String(snapshot.definition_id.as_str().to_string()),
    );
    if let Some(assignment) = &snapshot.assignment {
        metadata.insert(
            META_AGENT_ID.to_string(),
            Value::String(assignment.agent.as_str().to_string()),
        );
    }
    metadata.insert(
        META_PROJECTION_REVISION.to_string(),
        Value::Number(projection_revision.into()),
    );
    metadata.insert(
        META_STATUS_SOURCE.to_string(),
        Value::String(AGENT_STATUS_SOURCE.to_string()),
    );
    A2ATaskProjection {
        task_id: snapshot.scope.task().as_str().to_string(),
        context_id: context_id.to_string(),
        tenant: tenant.to_string(),
        workflow_id: snapshot.definition_id.as_str().to_string(),
        status: agent_task_state(condition),
        status_timestamp: snapshot.updated_at,
        history,
        artifacts: Vec::new(),
        metadata,
        projection_revision,
    }
}

/// Projects a durably accepted agent send into the projection store,
/// returning the public events it emitted.
///
/// Mirrors the substrate surface: a missing projection is bootstrapped with
/// a snapshot event carrying the accepted message; an existing projection
/// gains the message (healing a lost projection write on retry, while
/// ordinary duplicates find it already recorded) and a status event only
/// when the public state actually changed.
pub(crate) async fn project_agent_send(
    store: &dyn A2ATaskProjectionStore,
    snapshot: &AgentTaskSnapshot,
    run: Option<AgentRunStatus>,
    tenant: &str,
    context_id: &str,
    message: &Message,
    now: AgentTimestampMillis,
) -> RakkaAgentA2AResult<Vec<A2ATaskEvent>> {
    let task_id = snapshot.scope.task().as_str();
    let mut events = Vec::new();
    match store.projection(Some(tenant), task_id).await {
        Ok(projection) => {
            let already_projected = projection
                .history
                .iter()
                .any(|recorded| recorded.message_id == message.message_id);
            if !already_projected {
                events.push(
                    store
                        .append_event_payload(
                            tenant,
                            task_id,
                            context_id,
                            now,
                            A2ATaskEventPayload::MessageUpdate {
                                message: message.clone(),
                            },
                        )
                        .await?,
                );
            }
            if let Some(event) = sync_agent_status(
                store,
                snapshot,
                run,
                tenant,
                context_id,
                now,
                Some(projection.status),
            )
            .await?
            {
                events.push(event);
            }
        }
        Err(TaskProjectionError::TaskNotFound { .. }) => {
            let projection = agent_projection_from_snapshot(
                snapshot,
                run,
                tenant,
                context_id,
                vec![message.clone()],
                0,
            );
            events.push(
                store
                    .append_event_payload(
                        tenant,
                        task_id,
                        context_id,
                        now,
                        A2ATaskEventPayload::Snapshot(projection),
                    )
                    .await?,
            );
        }
        Err(error) => return Err(error.into()),
    }
    Ok(events)
}

/// Brings the projection in line with the authoritative snapshot, creating
/// it when missing and appending a status event only on a real, allowed
/// public transition.
pub(crate) async fn sync_agent_status(
    store: &dyn A2ATaskProjectionStore,
    snapshot: &AgentTaskSnapshot,
    run: Option<AgentRunStatus>,
    tenant: &str,
    context_id: &str,
    now: AgentTimestampMillis,
    current_status: Option<a2a::TaskState>,
) -> RakkaAgentA2AResult<Option<A2ATaskEvent>> {
    let task_id = snapshot.scope.task().as_str();
    let state = agent_task_state(AgentTaskCondition {
        task: snapshot.status,
        run,
    });
    let current = match current_status {
        Some(status) => status,
        None => match store.projection(Some(tenant), task_id).await {
            Ok(projection) => projection.status,
            Err(TaskProjectionError::TaskNotFound { .. }) => {
                let projection = agent_projection_from_snapshot(
                    snapshot,
                    run,
                    tenant,
                    context_id,
                    Vec::new(),
                    0,
                );
                return store
                    .append_event_payload(
                        tenant,
                        task_id,
                        context_id,
                        now,
                        A2ATaskEventPayload::Snapshot(projection),
                    )
                    .await
                    .map(Some)
                    .map_err(Into::into);
            }
            Err(error) => return Err(error.into()),
        },
    };
    if current == state || !status_transition_allowed(&current, &state) {
        return Ok(None);
    }
    let payload = if state.is_terminal() {
        A2ATaskEventPayload::Terminal { state }
    } else {
        A2ATaskEventPayload::StatusUpdate { state }
    };
    store
        .append_event_payload(tenant, task_id, context_id, now, payload)
        .await
        .map(Some)
        .map_err(Into::into)
}
