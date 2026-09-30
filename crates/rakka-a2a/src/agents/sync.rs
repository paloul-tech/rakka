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
    // envelope. The clusters merge under the one key — a delegated task
    // that was later handed off echoes both, and a governed conversation's
    // terminal echo rides beside them.
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
    if let Some(cell) = snapshot.conversation.as_deref() {
        if let Value::Object(echo) = super::collaboration::conversation_echo(cell) {
            collaboration.extend(echo);
        }
    }
    if !collaboration.is_empty() {
        metadata.insert(
            super::collaboration::META_COLLABORATION.to_string(),
            Value::Object(collaboration),
        );
    }
    // The bounded rejection echo (specification 8.12): a rejected
    // typed-result submission answers with the ordinary task view, so the
    // rule code must ride the view. Assembled here — never a bootstrap-only
    // write — so the metadata half of `sync_agent_status` heals it on every
    // read and write path.
    if snapshot.rejection_count > 0 {
        metadata.insert(
            super::projection::META_AGENT_REJECTIONS.to_string(),
            Value::Number(snapshot.rejection_count.into()),
        );
    }
    if let Some(rejection) = snapshot.last_rejection.as_deref() {
        let mut echo = serde_json::Map::new();
        echo.insert(
            "reason".to_string(),
            Value::String(rejection.cause.reason.clone()),
        );
        if let Some(rule) = &rejection.cause.rule_id {
            echo.insert("rule".to_string(), Value::String(rule.as_str().to_string()));
        }
        metadata.insert(
            super::projection::META_AGENT_LAST_REJECTION.to_string(),
            Value::Object(echo),
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

/// Bounded retry budget for the sequence-claimed appends below. Every write
/// that is *computed from* a loaded projection claims that copy's next
/// sequence explicitly, so a concurrent writer surfaces as an event-order
/// conflict and the loser reloads the store head instead of publishing state
/// derived from a stale copy. Contention beyond this many rounds is surfaced
/// as the conflict rather than spun on.
const SYNC_APPEND_ATTEMPTS: usize = 3;

/// Projects a durably accepted agent send into the projection store,
/// returning the public events it emitted.
///
/// Mirrors the substrate surface: a missing projection is bootstrapped with
/// a snapshot event carrying the accepted message; an existing projection
/// gains the message (healing a lost projection write on retry, while
/// ordinary duplicates find it already recorded) and a status event only
/// when the public state actually changed. The bootstrap claims sequence one
/// explicitly, so losing a concurrent bootstrap race reloads and joins the
/// winner's projection instead of adopting a second snapshot over it.
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
    let mut conflicts = 0;
    loop {
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
                            projection.context_id.as_str(),
                            now,
                            A2ATaskEventPayload::MessageUpdate {
                                message: message.clone(),
                            },
                        )
                        .await?;
                    // Keep the local copy in step with what the store now
                    // holds, so the status/metadata sync below compares —
                    // and, on a refresh, re-snapshots — the history including
                    // this message. A concurrent writer can outrun the copy;
                    // the sync then reloads instead of trusting it.
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
                return Ok(events);
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
                let event = A2ATaskEvent::new(
                    tenant,
                    task_id,
                    context_id,
                    1,
                    now,
                    A2ATaskEventPayload::Snapshot(projection),
                );
                match store.append_event(event).await {
                    Ok(event) => {
                        events.push(event);
                        return Ok(events);
                    }
                    Err(error @ TaskProjectionError::EventOrder { .. }) => {
                        // Lost the bootstrap race: a concurrent writer
                        // created the projection after the read above.
                        // Reload and join what it holds rather than adopting
                        // this snapshot over it.
                        conflicts += 1;
                        if conflicts >= SYNC_APPEND_ATTEMPTS {
                            return Err(error.into());
                        }
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
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
///
/// Every write claims the loaded copy's next sequence, and a refresh is
/// rebuilt from the loaded head — its context identity, its history, its
/// status — never from what a caller derived. A concurrent writer therefore
/// surfaces as an event-order conflict that reloads and recomputes, instead
/// of a stale snapshot adoption that would regress a terminal status, drop
/// concurrently appended history, or overwrite the stored `context_id` with
/// a read path's task-id default.
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
    let mut events = Vec::new();
    let mut carried = current.cloned();
    let mut conflicts = 0;
    loop {
        let mut current = match carried.take() {
            Some(projection) => projection,
            None => match store.projection(Some(tenant), task_id).await {
                Ok(projection) => projection,
                Err(TaskProjectionError::TaskNotFound { .. }) => {
                    let projection = agent_projection_from_snapshot(
                        snapshot,
                        run,
                        tenant,
                        context_id,
                        Vec::new(),
                        0,
                    );
                    let event = A2ATaskEvent::new(
                        tenant,
                        task_id,
                        context_id,
                        1,
                        now,
                        A2ATaskEventPayload::Snapshot(projection),
                    );
                    match store.append_event(event).await {
                        Ok(event) => {
                            events.push(event);
                            return Ok(events);
                        }
                        Err(error @ TaskProjectionError::EventOrder { .. }) => {
                            // Lost the bootstrap race; reload and sync onto
                            // the winner's projection.
                            conflicts += 1;
                            if conflicts >= SYNC_APPEND_ATTEMPTS {
                                return Err(error.into());
                            }
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                Err(error) => return Err(error.into()),
            },
        };
        // The status half: claimed at the loaded copy's next sequence, so a
        // transition is never decided against a head another writer already
        // moved — a lost claim reloads and re-decides.
        if current.status != state && status_transition_allowed(&current.status, &state) {
            let payload = if state.is_terminal() {
                A2ATaskEventPayload::Terminal {
                    state: state.clone(),
                }
            } else {
                A2ATaskEventPayload::StatusUpdate {
                    state: state.clone(),
                }
            };
            let event = A2ATaskEvent::new(
                tenant,
                task_id,
                current.context_id.as_str(),
                current.projection_revision.saturating_add(1),
                now,
                payload,
            );
            match store.append_event(event).await {
                Ok(event) => {
                    // Infallible by construction: the event claims exactly
                    // this copy's next sequence, and the transition was
                    // checked against this copy's status.
                    let _ = current.apply_event(&event);
                    events.push(event);
                }
                Err(error @ TaskProjectionError::EventOrder { .. }) => {
                    conflicts += 1;
                    if conflicts >= SYNC_APPEND_ATTEMPTS {
                        return Err(error.into());
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }
        // The metadata half: the refresh keeps the head's context identity,
        // history, and (possibly just transitioned) status — only the
        // snapshot-assembled metadata is what it exists to bring in line.
        if agent_metadata_from_snapshot(snapshot, run, current.projection_revision)
            != current.metadata
        {
            let mut refreshed = agent_projection_from_snapshot(
                snapshot,
                run,
                tenant,
                current.context_id.as_str(),
                current.history.clone(),
                current.projection_revision,
            );
            refreshed.status = current.status.clone();
            let event = A2ATaskEvent::new(
                tenant,
                task_id,
                current.context_id.as_str(),
                current.projection_revision.saturating_add(1),
                now,
                A2ATaskEventPayload::Snapshot(refreshed),
            );
            match store.append_event(event).await {
                Ok(event) => events.push(event),
                Err(error @ TaskProjectionError::EventOrder { .. }) => {
                    conflicts += 1;
                    if conflicts >= SYNC_APPEND_ATTEMPTS {
                        return Err(error.into());
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }
        return Ok(events);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use a2a::{Part, PartContent, Role, TaskState};
    use serde_json::json;

    use rakka_agent::testkit::{
        DeferredExchangeRouter, InProcessRunEntityTransport, InProcessTaskEntityTransport,
    };
    use rakka_agent::{
        AgentAuthorityEnvelope, AgentDefinition, AgentDefinitionId, AgentEntityClass,
        AgentEntityCommand, AgentEntityState, AgentEntityStore, AgentExchangeRouter, AgentId,
        AgentOperationId, AgentOperationKind, AgentRevisionNumber, AgentRevisionProvenance,
        AgentRunState, AgentSchemaId, AgentSchemaRef, AgentScope, AgentSettings, AgentTaskContent,
        AgentTaskCreation, AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskEntityCommand,
        AgentTaskEntityStore, AgentTaskId, AgentTaskScope, AgentTaskState,
        InMemoryAgentRunEffectSink, InMemoryAgentTaskHistoryStore, TenantId,
    };
    use rakka_persistence::InMemoryDurableStateStore;

    use crate::projection::InMemoryA2ATaskProjectionStore;

    const TENANT: &str = "acme";
    const AGENT: &str = "support-agent";
    const TASK: &str = "ticket-sync";
    const TASK_DEFINITION: &str = "resolve-ticket";

    fn schema(id: &str) -> AgentSchemaRef {
        AgentSchemaRef::new(
            AgentSchemaId::new(id).expect("schema id should be valid"),
            AgentRevisionNumber::INITIAL,
        )
    }

    /// A real created-task snapshot, driven through the task entity so the
    /// fixture never guesses at snapshot invariants.
    async fn created_task_snapshot() -> AgentTaskSnapshot {
        let tasks: InMemoryDurableStateStore<AgentTaskState> = InMemoryDurableStateStore::new();
        let agents: InMemoryDurableStateStore<AgentEntityState> = InMemoryDurableStateStore::new();
        let runs: InMemoryDurableStateStore<AgentRunState> = InMemoryDurableStateStore::new();
        let history = InMemoryAgentTaskHistoryStore::new();
        let effects = InMemoryAgentRunEffectSink::new();
        let clock = Arc::new(AtomicU64::new(1));
        let deferred = DeferredExchangeRouter::new();
        let task_transport = InProcessTaskEntityTransport::new(
            tasks.clone(),
            agents.clone(),
            history.clone(),
            deferred.as_router(),
            clock.clone(),
        );
        let run_transport = InProcessRunEntityTransport::new(
            runs.clone(),
            effects.clone(),
            deferred.as_router(),
            clock.clone(),
        );
        let router = AgentExchangeRouter::new()
            .with_route(AgentEntityClass::Task, Arc::new(task_transport))
            .with_route(AgentEntityClass::Run, Arc::new(run_transport));
        deferred.install(router.clone());

        let agent = AgentId::new(AGENT).expect("agent id should be valid");
        let agent_scope =
            AgentScope::new(TenantId::new(TENANT), agent.clone()).expect("agent scope");
        let definition_id =
            AgentTaskDefinitionId::new(TASK_DEFINITION).expect("definition id should be valid");
        let mut envelope = AgentAuthorityEnvelope::empty();
        envelope.task_definitions.insert(definition_id.clone());
        let mut entity = AgentEntityStore::new(agent_scope.clone(), agents.clone());
        entity.recover().await.expect("the agent recovers");
        entity
            .apply(AgentEntityCommand::Instantiate {
                operation_id: AgentOperationId::for_agent(
                    AgentOperationKind::DefinitionUpdate,
                    &agent_scope,
                    "1",
                )
                .expect("operation id should be derivable"),
                definition: Box::new(
                    AgentDefinition::new(
                        AgentDefinitionId::new("support-v1").expect("definition id"),
                        "One projected agent.",
                        envelope,
                    )
                    .expect("the agent definition is valid"),
                ),
                settings: Box::new(AgentSettings::default()),
                provenance: Box::new(AgentRevisionProvenance {
                    principal: rakka_agent_workflow::PrincipalRef {
                        principal_type: "user".to_string(),
                        principal_id: "operator-7".to_string(),
                        display_name: None,
                    },
                    accepted_at: AgentTimestampMillis::new(1),
                    causation_id: rakka_agent_workflow::AgentCausationId::new("cause-1"),
                    audit_ref: rakka_agent_workflow::AgentAuditEventId::new("audit-1"),
                }),
            })
            .await
            .expect("the agent instantiates");

        let scope = AgentTaskScope::new(
            TenantId::new(TENANT),
            AgentTaskId::new(TASK).expect("task id should be valid"),
        )
        .expect("task scope should be valid");
        let mut task = AgentTaskEntityStore::new(scope, tasks.clone(), agents.clone(), history);
        let now = AgentTimestampMillis::new(clock.fetch_add(1, Ordering::SeqCst));
        task.recover(now).await.expect("the task recovers");
        task.apply(
            AgentTaskEntityCommand::Create {
                operation_id: AgentOperationId::new(
                    AgentOperationKind::TaskCreation,
                    [TENANT, TASK, "1"],
                )
                .expect("operation id should be derivable"),
                creation: Box::new(AgentTaskCreation {
                    definition: AgentTaskDefinition::new(
                        definition_id,
                        "One projected ticket.",
                        schema("input"),
                        schema("result"),
                    )
                    .expect("the task definition is valid"),
                    input: AgentTaskContent::inline(json!({ "ticket": 1 }))
                        .expect("the input is inline-bounded"),
                    assignee: Some(agent),
                    team: None,
                    goal: None,
                    goal_mode: Default::default(),
                    goal_spec: None,
                    parent: None,
                    dependencies: Vec::new(),
                    escrow: None,
                    wake: None,
                    delegation: None,
                    telemetry: Default::default(),
                }),
            },
            &router,
            AgentTimestampMillis::new(clock.fetch_add(1, Ordering::SeqCst)),
        )
        .await
        .expect("the task creates");
        task.snapshot()
            .expect("the snapshot reads")
            .expect("the task exists")
    }

    /// The drift-refresh race: a concurrent writer appends between the
    /// caller's load and its refresh. The sequence-claimed appends make the
    /// stale caller reload instead of adopting its copy over the head —
    /// keeping the concurrently appended terminal status and history and the
    /// stored context identity, while still landing the metadata refresh.
    #[tokio::test]
    async fn a_stale_copy_cannot_regress_the_projection_head() {
        let snapshot = created_task_snapshot().await;
        let task_id = snapshot.scope.task().as_str().to_string();
        let store = InMemoryA2ATaskProjectionStore::local();

        // Bootstrap under the created context.
        sync_agent_status(
            &store,
            &snapshot,
            None,
            TENANT,
            "conv-1",
            AgentTimestampMillis::new(10),
            None,
        )
        .await
        .expect("the projection bootstraps");
        let stale = store
            .projection(Some(TENANT), &task_id)
            .await
            .expect("the projection reads");

        // The concurrent writer outruns the stale copy: one message, then
        // the terminal transition.
        let mut message = Message::new(
            Role::User,
            vec![Part {
                content: PartContent::Data(json!({ "note": "resolved" })),
                filename: None,
                media_type: Some("application/json".to_string()),
                metadata: None,
            }],
        );
        message.message_id = "concurrent-1".to_string();
        store
            .append_event_payload(
                TENANT,
                &task_id,
                "conv-1",
                AgentTimestampMillis::new(11),
                A2ATaskEventPayload::MessageUpdate {
                    message: message.clone(),
                },
            )
            .await
            .expect("the concurrent message appends");
        store
            .append_event_payload(
                TENANT,
                &task_id,
                "conv-1",
                AgentTimestampMillis::new(12),
                A2ATaskEventPayload::Terminal {
                    state: TaskState::Completed,
                },
            )
            .await
            .expect("the concurrent terminal appends");

        // The stale caller syncs a drifted condition (a run status appeared)
        // under the read path's task-id context default.
        let events = sync_agent_status(
            &store,
            &snapshot,
            Some(AgentRunStatus::Running),
            TENANT,
            &task_id,
            AgentTimestampMillis::new(13),
            Some(&stale),
        )
        .await
        .expect("the stale sync converges");
        assert!(
            events
                .iter()
                .all(|event| matches!(event.payload, A2ATaskEventPayload::Snapshot(_))),
            "no stale status event is published, got {events:?}"
        );

        let healed = store
            .projection(Some(TENANT), &task_id)
            .await
            .expect("the projection reads");
        assert_eq!(
            healed.status,
            TaskState::Completed,
            "the concurrent terminal stands"
        );
        assert_eq!(
            healed.context_id, "conv-1",
            "the stored context identity stands"
        );
        assert!(
            healed
                .history
                .iter()
                .any(|recorded| recorded.message_id == "concurrent-1"),
            "the concurrently appended history stands"
        );
        assert_eq!(
            healed.metadata,
            agent_metadata_from_snapshot(
                &snapshot,
                Some(AgentRunStatus::Running),
                healed.projection_revision
            ),
            "the drifted metadata was brought in line"
        );
    }
}
