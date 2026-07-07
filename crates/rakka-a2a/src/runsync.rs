//! Shared run-to-projection synchronization logic.
//!
//! Used by both the public request handler (local acceptance path) and the
//! sharded run owner entity, so both paths produce identical public event
//! streams from identical durable transitions.

use a2a::{Message, TaskState};
use rakka_agent_workflow::substrate::{DeduplicationKey, WorkflowMessageId, WorkflowState};
use rakka_agent_workflow::{
    AgentCommand, AgentCommandKind, AgentRunId, AgentRunInbox, AgentRunState, AgentRunStatus,
    AgentStatePayload, AgentTimestampMillis, ArtifactRef,
};
use rakka_persistence::DurableStateStore;

use crate::error::RakkaA2AHandlerError;
use crate::mapping::{
    A2ACommandDraft, A2ACommandPayload, A2ATaskIntent, ATTR_CONTEXT_ID, DEFAULT_TENANT,
};
use crate::projection::A2ATaskProjectionStore;
use crate::task::{
    status_transition_allowed, task_state_from_run_status, A2ATaskEvent, A2ATaskEventPayload,
    A2ATaskProjection, TaskProjectionError,
};

/// Projects a durably accepted send into the task store, returning the
/// public events it emitted.
pub(crate) async fn project_send_result(
    task_store: &dyn A2ATaskProjectionStore,
    draft: &A2ACommandDraft,
    message: &Message,
    artifacts: Vec<ArtifactRef>,
    run_state: &AgentRunState,
    now: AgentTimestampMillis,
) -> Result<Vec<A2ATaskEvent>, RakkaA2AHandlerError> {
    let tenant = draft.normalized.tenant.as_str();
    let mut events = Vec::new();
    match task_store
        .projection(Some(tenant), &draft.normalized.task_id)
        .await
    {
        Ok(projection) => {
            // The message is appended by history presence, not by fresh
            // acceptance: a durably accepted command whose projection
            // write was lost must be healed by its retry, while ordinary
            // duplicates find their message already recorded. (A message
            // evicted from the bounded history would be re-appended by a
            // very late duplicate; acceptable for this read model.)
            let already_projected = projection
                .history
                .iter()
                .any(|recorded| recorded.message_id == message.message_id);
            if !already_projected {
                events.push(
                    task_store
                        .append_event_payload(
                            tenant,
                            &draft.normalized.task_id,
                            &draft.normalized.context_id,
                            now,
                            A2ATaskEventPayload::MessageUpdate {
                                message: message.clone(),
                            },
                        )
                        .await?,
                );
            }
            if let Some(event) = sync_status_projection(
                task_store,
                run_state,
                &draft.normalized.context_id,
                now,
                Some(projection.status),
            )
            .await?
            {
                events.push(event);
            }
            Ok(events)
        }
        Err(TaskProjectionError::TaskNotFound { .. }) => {
            events.push(
                snapshot_projection(
                    task_store,
                    run_state,
                    &draft.normalized.context_id,
                    vec![message.clone()],
                    artifacts,
                    now,
                )
                .await?,
            );
            Ok(events)
        }
        Err(error) => Err(error.into()),
    }
}

/// Brings the task projection in line with durable run state, creating it
/// when missing and appending a status event only when the public task
/// state actually changed. `current_status` skips the projection read
/// when the caller already holds it.
pub(crate) async fn sync_status_projection(
    task_store: &dyn A2ATaskProjectionStore,
    run_state: &AgentRunState,
    context_id: &str,
    now: AgentTimestampMillis,
    current_status: Option<TaskState>,
) -> Result<Option<A2ATaskEvent>, RakkaA2AHandlerError> {
    let tenant = run_tenant(run_state);
    let state = task_state_from_run_status(run_state.status);
    let current = match current_status {
        Some(status) => status,
        None => match task_store
            .projection(Some(&tenant), run_state.run_id.as_str())
            .await
        {
            Ok(projection) => projection.status,
            Err(TaskProjectionError::TaskNotFound { .. }) => {
                return snapshot_projection(
                    task_store,
                    run_state,
                    context_id,
                    Vec::new(),
                    Vec::new(),
                    now,
                )
                .await
                .map(Some);
            }
            Err(error) => return Err(error.into()),
        },
    };
    if current == state {
        return Ok(None);
    }
    // The shared no-regression rule (also enforced inside apply_event)
    // is checked here first so a disallowed transition — e.g. from a
    // stale run-state snapshot — appends no event at all.
    if !status_transition_allowed(&current, &state) {
        return Ok(None);
    }
    let payload = if state.is_terminal() {
        A2ATaskEventPayload::Terminal { state }
    } else {
        A2ATaskEventPayload::StatusUpdate { state }
    };
    task_store
        .append_event_payload(
            tenant.as_str(),
            run_state.run_id.as_str(),
            context_id,
            now,
            payload,
        )
        .await
        .map(Some)
        .map_err(Into::into)
}

/// Appends a snapshot event rebuilt from durable run state, bootstrapping
/// the projection for tasks the store has not seen.
pub(crate) async fn snapshot_projection(
    task_store: &dyn A2ATaskProjectionStore,
    run_state: &AgentRunState,
    context_id: &str,
    history: Vec<Message>,
    artifacts: Vec<ArtifactRef>,
    now: AgentTimestampMillis,
) -> Result<A2ATaskEvent, RakkaA2AHandlerError> {
    let projection =
        A2ATaskProjection::from_run_state(run_state, context_id, history, artifacts, 0);
    let tenant = projection.tenant.clone();
    let task_id = projection.task_id.clone();
    let context_id = projection.context_id.clone();
    task_store
        .append_event_payload(
            &tenant,
            &task_id,
            &context_id,
            now,
            A2ATaskEventPayload::Snapshot(projection),
        )
        .await
        .map_err(Into::into)
}

/// Validates a send request's lifecycle intent against recovered run state.
pub(crate) fn validate_send_lifecycle(
    draft: &A2ACommandDraft,
    run_state: Option<&AgentRunState>,
) -> Result<(), RakkaA2AHandlerError> {
    if matches!(draft.normalized.intent, A2ATaskIntent::NewTask)
        && !matches!(&draft.command.kind, AgentCommandKind::StartRun)
    {
        return Err(RakkaA2AHandlerError::InvalidLifecycle {
            task_id: draft.normalized.task_id.clone(),
            reason: "new A2A tasks must map to StartRun",
        });
    }
    match run_state {
        // A run owned by another tenant is indistinguishable from a missing
        // task to this caller.
        Some(state) if run_tenant(state) != draft.normalized.tenant.as_str() => {
            Err(RakkaA2AHandlerError::MissingRun {
                task_id: draft.normalized.task_id.clone(),
            })
        }
        // Terminal tasks reject new messages before anything is accepted
        // durably, mirroring the terminal handling on the cancel path.
        Some(state)
            if matches!(draft.normalized.intent, A2ATaskIntent::ContinueTask)
                && run_is_terminal(state.status) =>
        {
            Err(RakkaA2AHandlerError::InvalidLifecycle {
                task_id: draft.normalized.task_id.clone(),
                reason: "messages cannot be sent to a task in a terminal state",
            })
        }
        None if matches!(draft.normalized.intent, A2ATaskIntent::ContinueTask) => {
            Err(RakkaA2AHandlerError::MissingRun {
                task_id: draft.normalized.task_id.clone(),
            })
        }
        _ => Ok(()),
    }
}

/// Re-validates a run adopted from a concurrent winner on the start path.
///
/// A retry of the same message adopts a run identical to the one this request
/// would have created; anything else means the hashed id collided with an
/// unrelated task. On rejection the command accepted just before the start
/// race remains in the adopted run's inbox — unreachable garbage that only a
/// hash collision plus a concurrent create can produce.
pub(crate) fn validate_adopted_run(
    state: &AgentRunState,
    draft: &A2ACommandDraft,
) -> Result<(), RakkaA2AHandlerError> {
    if run_tenant(state) != draft.normalized.tenant.as_str() {
        return Err(missing_run(&draft.normalized.task_id));
    }
    if !same_state_payload(&state.state_payload, &state_payload(&draft.payload)) {
        return Err(RakkaA2AHandlerError::InvalidLifecycle {
            task_id: draft.normalized.task_id.clone(),
            reason: "generated task id collides with an existing task",
        });
    }
    Ok(())
}

/// Compares run payloads semantically rather than byte-for-byte.
///
/// Inline payloads hold a serialized A2A message whose map fields (`Message`
/// and `Part` metadata) have no deterministic wire ordering, so identical
/// messages can serialize to different bytes; deserialize and compare the
/// messages instead, falling back to byte equality for non-message payloads.
pub(crate) fn same_state_payload(
    existing: &AgentStatePayload,
    candidate: &AgentStatePayload,
) -> bool {
    match (existing, candidate) {
        (AgentStatePayload::Inline(existing), AgentStatePayload::Inline(candidate)) => {
            if existing.content_type != candidate.content_type {
                return false;
            }
            match (
                serde_json::from_slice::<Message>(&existing.bytes),
                serde_json::from_slice::<Message>(&candidate.bytes),
            ) {
                (Ok(existing), Ok(candidate)) => existing == candidate,
                _ => existing.bytes == candidate.bytes,
            }
        }
        (existing, candidate) => existing == candidate,
    }
}

/// Returns true when the run's durable inbox already holds this command,
/// using the inbox's own keyed lookups so the match cannot drift from the
/// acceptance-time duplicate detection.
pub(crate) fn known_command(state: &WorkflowState, draft: &A2ACommandDraft) -> bool {
    let command_id = WorkflowMessageId::new(draft.command.metadata.command_id.as_str());
    let deduplication_key =
        DeduplicationKey::new(draft.command.metadata.deduplication_key.as_str());
    state.inbox_entry(&command_id).is_some()
        || state
            .inbox_entry_by_deduplication_key(&deduplication_key)
            .is_some()
}

/// Recovers the original A2A context id from the run's durable inbox.
pub(crate) async fn recover_context_id<WorkflowStoreT>(
    workflow_store: &WorkflowStoreT,
    run_id: &AgentRunId,
) -> Result<Option<String>, RakkaA2AHandlerError>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    let mut inbox = AgentRunInbox::new(run_id.clone(), workflow_store.clone());
    let state = inbox.recover().await?;
    let mut fallback = None;
    for entry in state.inbox().values() {
        let command = match serde_json::from_slice::<AgentCommand>(entry.payload()) {
            Ok(command) => command,
            Err(error) => {
                // Surface undecodable durable payloads instead of silently
                // degrading recovery.
                eprintln!(
                    "warning: skipping undecodable inbox entry {} for run {}: {error}",
                    entry.message_id().as_str(),
                    run_id.as_str(),
                );
                continue;
            }
        };
        let context_id = command.attributes.get(ATTR_CONTEXT_ID).cloned();
        if context_id.is_none() {
            continue;
        }
        if matches!(command.kind, AgentCommandKind::StartRun) {
            return Ok(context_id);
        }
        fallback = fallback.or(context_id);
    }
    Ok(fallback)
}

/// Constructs the task-not-found error used for missing and foreign-tenant runs.
pub(crate) fn missing_run(task_id: &str) -> RakkaA2AHandlerError {
    RakkaA2AHandlerError::MissingRun {
        task_id: task_id.to_string(),
    }
}

/// Resolves the run's tenant, defaulting for single-tenant/local runs.
pub(crate) fn run_tenant(run_state: &AgentRunState) -> String {
    run_state
        .tenant
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| DEFAULT_TENANT.to_string())
}

/// True when the run status is terminal.
pub(crate) fn run_is_terminal(status: AgentRunStatus) -> bool {
    matches!(
        status,
        AgentRunStatus::Completed | AgentRunStatus::Failed | AgentRunStatus::Cancelled
    )
}

/// Extracts the durable state payload from a command payload draft.
pub(crate) fn state_payload(payload: &A2ACommandPayload) -> AgentStatePayload {
    match payload {
        A2ACommandPayload::Inline(inline) => AgentStatePayload::Inline(inline.clone()),
        A2ACommandPayload::ArtifactDrafts(drafts) => drafts
            .first()
            .map(|draft| AgentStatePayload::Artifact(draft.reference.clone()))
            .unwrap_or(AgentStatePayload::Empty),
        A2ACommandPayload::Empty => AgentStatePayload::Empty,
    }
}

/// Extracts the artifact references from a command payload draft.
pub(crate) fn artifact_refs(payload: &A2ACommandPayload) -> Vec<ArtifactRef> {
    payload
        .artifact_drafts()
        .iter()
        .map(|draft| draft.reference.clone())
        .collect()
}

/// Rewrites the public message with the normalized task and context ids.
///
/// Only the ingress request handler projects messages; the owner receives an
/// already-projected message, so this stays server-only.
#[cfg(feature = "server")]
pub(crate) fn projected_message(message: &Message, draft: &A2ACommandDraft) -> Message {
    let mut message = message.clone();
    message.task_id = Some(draft.normalized.task_id.clone());
    message.context_id = Some(draft.normalized.context_id.clone());
    message
}

/// Builds the initial durable run state for a newly accepted A2A task.
pub(crate) fn initial_run_state(
    workflow: &rakka_agent_workflow::AgentWorkflow,
    draft: &A2ACommandDraft,
    now: AgentTimestampMillis,
) -> Result<AgentRunState, RakkaA2AHandlerError> {
    let current_step_id = workflow
        .steps
        .first()
        .map(|step| step.step_id.clone())
        .ok_or_else(|| RakkaA2AHandlerError::InvalidLifecycle {
            task_id: draft.normalized.task_id.clone(),
            reason: "workflow has no executable steps",
        })?;
    let artifacts = artifact_refs(&draft.payload);
    Ok(AgentRunState {
        run_id: draft.normalized.run_id(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(draft.normalized.tenant.clone()),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        graph_state: None,
        status: AgentRunStatus::Accepted,
        current_step_id: Some(current_step_id),
        current_attempt: 0,
        inputs_ref: artifacts.first().cloned(),
        state_payload: state_payload(&draft.payload),
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: now,
        updated_at: now,
        completed_at: None,
    })
}
