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

/// Assembles the bounded public metadata for one authoritative snapshot —
/// the map every projection assembled from that snapshot carries.
#[must_use]
fn agent_metadata_from_snapshot(
    snapshot: &AgentTaskSnapshot,
    run: Option<AgentRunStatus>,
    projection_revision: u64,
) -> HashMap<String, Value> {
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
    // The bounded collaboration echo: enough for observability and the
    // delegating or handing-off sender's identity check, never the whole
    // envelope. The two clusters merge under the one key — a delegated task
    // that was later handed off echoes both.
    let mut collaboration = serde_json::Map::new();
    if let Some(provenance) = snapshot.delegation.as_deref() {
        if let Value::Object(echo) = super::collaboration::collaboration_echo(provenance) {
            collaboration.extend(echo);
        }
    }
    if let Some(handoff) = snapshot.handoff.as_deref() {
        if let Value::Object(echo) = super::collaboration::handoff_echo(handoff) {
            collaboration.extend(echo);
        }
    }
    if let Some(claim) = snapshot.team_claim.as_deref() {
        if let Value::Object(echo) = super::collaboration::team_echo(claim) {
            collaboration.extend(echo);
        }
    }
    if !collaboration.is_empty() {
        metadata.insert(
            super::collaboration::META_COLLABORATION.to_string(),
            Value::Object(collaboration),
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
    metadata
}

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
    A2ATaskProjection {
        task_id: snapshot.scope.task().as_str().to_string(),
        context_id: context_id.to_string(),
        tenant: tenant.to_string(),
        workflow_id: snapshot.definition_id.as_str().to_string(),
        status: agent_task_state(condition),
        status_timestamp: snapshot.updated_at,
        history,
        artifacts: Vec::new(),
        metadata: agent_metadata_from_snapshot(snapshot, run, projection_revision),
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
        Ok(mut projection) => {
            let already_projected = projection
                .history
                .iter()
                .any(|recorded| recorded.message_id == message.message_id);
            let mut current = true;
            if !already_projected {
                let event = store
                    .append_event_payload(
                        tenant,
                        task_id,
                        context_id,
                        now,
                        A2ATaskEventPayload::MessageUpdate {
                            message: message.clone(),
                        },
                    )
                    .await?;
                // Keep the local copy in step with what the store now holds,
                // so the status/metadata sync below compares — and, on a
                // refresh, re-snapshots — the history including this message.
                // A concurrent writer can outrun the copy; the sync then
                // reloads instead of trusting it.
                current = projection.apply_event(&event).is_ok();
                events.push(event);
            }
            events.extend(
                sync_agent_status(
                    store,
                    snapshot,
                    run,
                    tenant,
                    context_id,
                    now,
                    current.then_some(&projection),
                )
                .await?,
            );
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

/// Brings the projection in line with the authoritative snapshot: creates it
/// when missing, appends a status event on a real, allowed public
/// transition, and re-snapshots the projection when its bounded metadata —
/// assignment agent, condition labels, the collaboration echo — no longer
/// matches what the snapshot assembles. The metadata half is what keeps a
/// pre-existing projection's handoff echo truthful: without it the echo is
/// written once at bootstrap and a later transfer never surfaces, which the
/// sending executor's identity check would misread as an unrecorded
/// transfer.
pub(crate) async fn sync_agent_status(
    store: &dyn A2ATaskProjectionStore,
    snapshot: &AgentTaskSnapshot,
    run: Option<AgentRunStatus>,
    tenant: &str,
    context_id: &str,
    now: AgentTimestampMillis,
    current: Option<&A2ATaskProjection>,
) -> RakkaAgentA2AResult<Vec<A2ATaskEvent>> {
    let task_id = snapshot.scope.task().as_str();
    let state = agent_task_state(AgentTaskCondition {
        task: snapshot.status,
        run,
    });
    let loaded;
    let current = match current {
        Some(projection) => projection,
        None => match store.projection(Some(tenant), task_id).await {
            Ok(projection) => {
                loaded = projection;
                &loaded
            }
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
                    .map(|event| vec![event])
                    .map_err(Into::into);
            }
            Err(error) => return Err(error.into()),
        },
    };
    let mut events = Vec::new();
    let status = if current.status != state && status_transition_allowed(&current.status, &state) {
        let payload = if state.is_terminal() {
            A2ATaskEventPayload::Terminal {
                state: state.clone(),
            }
        } else {
            A2ATaskEventPayload::StatusUpdate {
                state: state.clone(),
            }
        };
        events.push(
            store
                .append_event_payload(tenant, task_id, context_id, now, payload)
                .await?,
        );
        state
    } else {
        // Either nothing moved or the transition is disallowed; the
        // projection's public status stands, and a metadata refresh below
        // must not regress it through snapshot adoption.
        current.status.clone()
    };
    if agent_metadata_from_snapshot(snapshot, run, current.projection_revision) != current.metadata
    {
        let mut refreshed = agent_projection_from_snapshot(
            snapshot,
            run,
            tenant,
            context_id,
            current.history.clone(),
            current.projection_revision,
        );
        refreshed.status = status;
        events.push(
            store
                .append_event_payload(
                    tenant,
                    task_id,
                    context_id,
                    now,
                    A2ATaskEventPayload::Snapshot(refreshed),
                )
                .await?,
        );
    }
    Ok(events)
}
