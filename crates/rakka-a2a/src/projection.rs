//! Task projection storage: async store trait, event watcher, retention, and
//! the in-memory implementation.
//!
//! The projection store is a query/observability read model over durable run
//! state. Any node may serve reads and stream replay from a shared store;
//! correctness always comes from durable run plus inbox/outbox state.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use a2a::{ListTasksRequest, ListTasksResponse, Task, TaskId};
use async_trait::async_trait;
use rakka_agent_workflow::AgentTimestampMillis;
use tokio::sync::broadcast;

use crate::task::{
    adopted_snapshot, page_offset, page_size, parse_replay_cursor, timestamp_to_datetime,
    A2ATaskEvent, A2ATaskEventPayload, A2ATaskProjection, TaskProjectionError,
    TaskProjectionResult,
};

const EVENT_WATCH_BUFFER: usize = 64;

/// Default maximum retained replay events per task; older events are dropped
/// first, preserving the newest snapshot for re-bootstrap.
pub const DEFAULT_EVENT_LOG_LIMIT: usize = 256;

/// Bounded retention for the per-task public event log.
///
/// Retention compacts event tails but must preserve terminal task snapshots
/// and replay-cursor behavior: replay from before the retained window reports
/// [`TaskProjectionError::ReplayWindowExpired`] instead of a silent gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct A2ATaskEventRetention {
    /// Maximum retained replay events per task.
    pub max_events_per_task: usize,
}

impl A2ATaskEventRetention {
    /// Creates a retention policy with the given per-task event bound.
    #[must_use]
    pub const fn new(max_events_per_task: usize) -> Self {
        Self {
            max_events_per_task,
        }
    }
}

impl Default for A2ATaskEventRetention {
    fn default() -> Self {
        Self::new(DEFAULT_EVENT_LOG_LIMIT)
    }
}

/// Async storage for A2A task projections and their public event logs.
///
/// Implementations must keep every query tenant-scoped when a tenant is
/// supplied; a tenant mismatch is indistinguishable from a missing task.
/// Stores constructed in tenant-scoped mode must reject unscoped
/// (`tenant = None`) reads with [`TaskProjectionError::TenantRequired`].
#[async_trait]
pub trait A2ATaskProjectionStore: Send + Sync + 'static {
    /// Stable backend name used in telemetry.
    fn backend_name(&self) -> &'static str;

    /// True when every node observes the same durable event log through this
    /// store, so cross-node stream replay can be served from the store
    /// without polling the shard owner.
    fn supports_shared_replay(&self) -> bool;

    /// True when this store refuses unscoped (`tenant = None`) reads.
    ///
    /// Shared multi-tenant stores must return `true` (the default);
    /// single-tenant/local stores may return `false` to permit unscoped
    /// reads. The service handler uses this to decide whether reads without
    /// tenant input stay unscoped or fall back to the single-tenant default.
    fn requires_tenant_scope(&self) -> bool {
        true
    }

    /// Inserts or replaces one projection without appending an event.
    async fn upsert(&self, projection: A2ATaskProjection) -> TaskProjectionResult<()>;

    /// Reads one raw projection record.
    async fn projection(
        &self,
        tenant: Option<&str>,
        task_id: &str,
    ) -> TaskProjectionResult<A2ATaskProjection>;

    /// Appends a payload as the next event for the task and returns the event.
    async fn append_event_payload(
        &self,
        tenant: &str,
        task_id: &str,
        context_id: &str,
        occurred_at: AgentTimestampMillis,
        payload: A2ATaskEventPayload,
    ) -> TaskProjectionResult<A2ATaskEvent>;

    /// Appends a public event, updating or bootstrapping the projection.
    ///
    /// Events for unknown tasks are rejected unless they carry a snapshot, so
    /// the replay log never records an event that no projection accepted.
    async fn append_event(&self, event: A2ATaskEvent) -> TaskProjectionResult<A2ATaskEvent>;

    /// Reads one task projection rendered as a bounded A2A `Task`.
    async fn get(
        &self,
        tenant: Option<&str>,
        task_id: &str,
        history_length: Option<i32>,
    ) -> TaskProjectionResult<Task> {
        self.projection(tenant, task_id)
            .await
            .map(|projection| projection.to_task(history_length, true))
    }

    /// Lists projections with deterministic pagination.
    async fn list(&self, request: &ListTasksRequest) -> TaskProjectionResult<ListTasksResponse>;

    /// Replays public task events after an optional cursor.
    ///
    /// Cursors are only valid for the task that minted them; a cursor from a
    /// different task is rejected instead of silently skipping events. A
    /// cursor older than the retained window yields
    /// [`TaskProjectionError::ReplayWindowExpired`].
    async fn replay_events(
        &self,
        tenant: &str,
        task_id: &str,
        after_cursor: Option<&str>,
    ) -> TaskProjectionResult<Vec<A2ATaskEvent>>;
}

/// Wake-up source for new durable task events.
///
/// Per the crate's replay design, a watcher only signals "there may be new
/// events at or after sequence S". The serving node then reads durable events
/// through [`A2ATaskProjectionStore::replay_events`]; it never treats the
/// notification payload as event data.
#[async_trait]
pub trait A2ATaskEventWatcher: Send + Sync + 'static {
    /// Opens a signal for future events of `(tenant, task_id)`.
    async fn watch(&self, tenant: &str, task_id: &str) -> TaskProjectionResult<A2ATaskEventSignal>;
}

/// Outcome of waiting on an [`A2ATaskEventSignal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum A2ATaskEventSignalOutcome {
    /// There may be new durable events at or after the hinted sequence.
    ///
    /// The hint is best effort (`0` when unknown); callers must replay from
    /// their own cursor rather than trust it.
    Notified {
        /// Best-effort high watermark observed by the watcher.
        high_watermark_hint: u64,
    },
    /// The watcher can no longer signal; callers should fall back to polling.
    Lost,
}

/// One subscription minted by an [`A2ATaskEventWatcher`].
pub struct A2ATaskEventSignal {
    source: Box<dyn A2ATaskEventSignalSource>,
}

impl std::fmt::Debug for A2ATaskEventSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("A2ATaskEventSignal")
    }
}

impl A2ATaskEventSignal {
    /// Wraps a custom signal source.
    #[must_use]
    pub fn from_source(source: Box<dyn A2ATaskEventSignalSource>) -> Self {
        Self { source }
    }

    /// Waits until there may be new durable events, or the watcher is lost.
    pub async fn changed(&mut self) -> A2ATaskEventSignalOutcome {
        self.source.changed().await
    }
}

/// Backing implementation for [`A2ATaskEventSignal`].
#[async_trait]
pub trait A2ATaskEventSignalSource: Send {
    /// Waits until there may be new durable events, or the watcher is lost.
    async fn changed(&mut self) -> A2ATaskEventSignalOutcome;
}

struct BroadcastSignalSource {
    receiver: broadcast::Receiver<u64>,
}

#[async_trait]
impl A2ATaskEventSignalSource for BroadcastSignalSource {
    async fn changed(&mut self) -> A2ATaskEventSignalOutcome {
        match self.receiver.recv().await {
            Ok(high_watermark_hint) => A2ATaskEventSignalOutcome::Notified {
                high_watermark_hint,
            },
            // Lag drops hints, never data: the caller replays durable events
            // from its own cursor, so a lagged signal is still just "wake up".
            Err(broadcast::error::RecvError::Lagged(_)) => A2ATaskEventSignalOutcome::Notified {
                high_watermark_hint: 0,
            },
            Err(broadcast::error::RecvError::Closed) => A2ATaskEventSignalOutcome::Lost,
        }
    }
}

/// In-memory projection store for tests, local mode, and single-node use.
#[derive(Debug, Clone)]
pub struct InMemoryA2ATaskProjectionStore {
    inner: Arc<Mutex<ProjectionStoreState>>,
    require_tenant_filter: bool,
    retention: A2ATaskEventRetention,
}

#[derive(Debug, Default)]
struct ProjectionStoreState {
    projections: BTreeMap<(String, TaskId), A2ATaskProjection>,
    events: BTreeMap<(String, TaskId), Vec<A2ATaskEvent>>,
    watchers: BTreeMap<(String, TaskId), broadcast::Sender<u64>>,
}

impl InMemoryA2ATaskProjectionStore {
    /// Creates a local-mode store that permits unscoped reads and listing.
    #[must_use]
    pub fn local() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProjectionStoreState::default())),
            require_tenant_filter: false,
            retention: A2ATaskEventRetention::default(),
        }
    }

    /// Creates a tenant-scoped store that requires tenant filters.
    #[must_use]
    pub fn tenant_scoped() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProjectionStoreState::default())),
            require_tenant_filter: true,
            retention: A2ATaskEventRetention::default(),
        }
    }

    /// Overrides the per-task event retention bound.
    #[must_use]
    pub fn with_retention(mut self, retention: A2ATaskEventRetention) -> Self {
        self.retention = retention;
        self
    }

    fn find_projection<'a>(
        state: &'a ProjectionStoreState,
        tenant: Option<&str>,
        task_id: &str,
    ) -> Option<&'a A2ATaskProjection> {
        match tenant {
            Some(tenant) => state
                .projections
                .get(&(tenant.to_string(), task_id.to_string())),
            None => state
                .projections
                .values()
                .find(|projection| projection.task_id == task_id),
        }
    }

    /// Applies one event under the store lock, bootstrapping only from
    /// snapshots, then wakes watchers with the new sequence.
    fn apply_event_locked(
        &self,
        state: &mut ProjectionStoreState,
        mut event: A2ATaskEvent,
    ) -> TaskProjectionResult<A2ATaskEvent> {
        let key = (event.tenant.clone(), event.task_id.clone());
        if let Some(projection) = state.projections.get_mut(&key) {
            projection.apply_event(&event)?;
            event.projected_state = projection.status.clone();
        } else if let A2ATaskEventPayload::Snapshot(snapshot) = &event.payload {
            let adopted = adopted_snapshot(snapshot, &event);
            event.projected_state = adopted.status.clone();
            state.projections.insert(key.clone(), adopted);
        } else {
            return Err(TaskProjectionError::TaskNotFound {
                task_id: event.task_id,
            });
        }
        let events = state.events.entry(key.clone()).or_default();
        events.push(event.clone());
        compact_event_log(events, self.retention.max_events_per_task);
        if let Some(sender) = state.watchers.get(&key) {
            // A send with no live receivers means every subscriber
            // disconnected; drop the sender so terminal tasks do not pin
            // watchers forever.
            if sender.send(event.sequence).is_err() {
                state.watchers.remove(&key);
            }
        }
        Ok(event)
    }

    #[cfg(test)]
    fn watcher_count(&self) -> usize {
        self.inner
            .lock()
            .expect("projection store mutex")
            .watchers
            .len()
    }
}

impl Default for InMemoryA2ATaskProjectionStore {
    fn default() -> Self {
        Self::local()
    }
}

#[async_trait]
impl A2ATaskProjectionStore for InMemoryA2ATaskProjectionStore {
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    fn supports_shared_replay(&self) -> bool {
        // Process-local: other nodes cannot observe this event log.
        false
    }

    fn requires_tenant_scope(&self) -> bool {
        self.require_tenant_filter
    }

    async fn upsert(&self, projection: A2ATaskProjection) -> TaskProjectionResult<()> {
        self.inner
            .lock()
            .expect("projection store mutex")
            .projections
            .insert(
                (projection.tenant.clone(), projection.task_id.clone()),
                projection,
            );
        Ok(())
    }

    async fn projection(
        &self,
        tenant: Option<&str>,
        task_id: &str,
    ) -> TaskProjectionResult<A2ATaskProjection> {
        if self.require_tenant_filter && tenant.is_none() {
            return Err(TaskProjectionError::TenantRequired);
        }
        let state = self.inner.lock().expect("projection store mutex");
        Self::find_projection(&state, tenant, task_id)
            .cloned()
            .ok_or_else(|| TaskProjectionError::TaskNotFound {
                task_id: task_id.to_string(),
            })
    }

    async fn append_event_payload(
        &self,
        tenant: &str,
        task_id: &str,
        context_id: &str,
        occurred_at: AgentTimestampMillis,
        payload: A2ATaskEventPayload,
    ) -> TaskProjectionResult<A2ATaskEvent> {
        let mut state = self.inner.lock().expect("projection store mutex");
        let key = (tenant.to_string(), task_id.to_string());
        let sequence = state.projections.get(&key).map_or(1, |projection| {
            projection.projection_revision.saturating_add(1)
        });
        let event = A2ATaskEvent::new(tenant, task_id, context_id, sequence, occurred_at, payload);
        self.apply_event_locked(&mut state, event)
    }

    async fn append_event(&self, event: A2ATaskEvent) -> TaskProjectionResult<A2ATaskEvent> {
        let mut state = self.inner.lock().expect("projection store mutex");
        self.apply_event_locked(&mut state, event)
    }

    async fn list(&self, request: &ListTasksRequest) -> TaskProjectionResult<ListTasksResponse> {
        if self.require_tenant_filter && request.tenant.is_none() {
            return Err(TaskProjectionError::TenantRequired);
        }
        let offset = page_offset(request.page_token.as_deref())?;
        let page_size = page_size(request.page_size);
        let after = request.status_timestamp_after;
        let state = self.inner.lock().expect("projection store mutex");
        let filtered = state
            .projections
            .values()
            .filter(|projection| {
                request
                    .tenant
                    .as_deref()
                    .is_none_or(|tenant| projection.tenant == tenant)
            })
            .filter(|projection| {
                request
                    .context_id
                    .as_deref()
                    .is_none_or(|context_id| projection.context_id == context_id)
            })
            .filter(|projection| {
                request
                    .status
                    .as_ref()
                    .is_none_or(|status| &projection.status == status)
            })
            .filter(|projection| {
                after.is_none_or(|after| {
                    timestamp_to_datetime(projection.status_timestamp)
                        .is_some_and(|timestamp| timestamp > after)
                })
            })
            // References only: the non-page remainder is never materialized.
            .collect::<Vec<_>>();
        let total_size = i32::try_from(filtered.len()).unwrap_or(i32::MAX);
        let tasks = filtered
            .iter()
            .skip(offset)
            .take(page_size)
            .map(|projection| {
                projection.to_task(
                    request.history_length,
                    request.include_artifacts.unwrap_or(false),
                )
            })
            .collect::<Vec<_>>();
        let next_offset = offset.saturating_add(tasks.len());
        let next_page_token = if next_offset < filtered.len() {
            next_offset.to_string()
        } else {
            String::new()
        };
        Ok(ListTasksResponse {
            tasks,
            next_page_token,
            page_size: i32::try_from(page_size).unwrap_or(i32::MAX),
            total_size,
        })
    }

    async fn replay_events(
        &self,
        tenant: &str,
        task_id: &str,
        after_cursor: Option<&str>,
    ) -> TaskProjectionResult<Vec<A2ATaskEvent>> {
        let after_sequence = match after_cursor {
            None => 0,
            Some(cursor) => {
                let (cursor_task_id, sequence) = parse_replay_cursor(cursor)?;
                if cursor_task_id != task_id {
                    return Err(TaskProjectionError::InvalidReplayCursor {
                        cursor: cursor.to_string(),
                    });
                }
                sequence
            }
        };
        let state = self.inner.lock().expect("projection store mutex");
        let key = (tenant.to_string(), task_id.to_string());
        // A cursor that points past everything this store has ever recorded
        // came from another node or an earlier owner epoch; it cannot prove
        // continuity here, so the caller must re-bootstrap from the snapshot.
        if after_sequence > 0 {
            let revision = state
                .projections
                .get(&key)
                .map(|projection| projection.projection_revision);
            if revision.is_none_or(|revision| after_sequence > revision) {
                return Err(TaskProjectionError::InvalidReplayCursor {
                    cursor: after_cursor.unwrap_or_default().to_string(),
                });
            }
        }
        let Some(events) = state.events.get(&key) else {
            if after_sequence == 0 {
                return Ok(Vec::new());
            }
            // The projection is known (e.g. cached from a routed owner
            // response) but no local event log covers the cursor, so replay
            // cannot resume incrementally from here.
            return Err(TaskProjectionError::ReplayWindowExpired {
                task_id: task_id.to_string(),
                earliest_sequence: 0,
            });
        };
        // The retained log is bounded; a request that starts before the
        // retained window would silently skip dropped events, so signal the
        // truncation and let the caller re-bootstrap from the current task.
        let mut expected = after_sequence.saturating_add(1);
        for event in events
            .iter()
            .filter(|event| event.sequence > after_sequence)
        {
            if event.sequence != expected {
                return Err(TaskProjectionError::ReplayWindowExpired {
                    task_id: task_id.to_string(),
                    earliest_sequence: event.sequence,
                });
            }
            expected = event.sequence.saturating_add(1);
        }
        Ok(events
            .iter()
            .filter(|event| event.sequence > after_sequence)
            .cloned()
            .collect())
    }
}

#[async_trait]
impl A2ATaskEventWatcher for InMemoryA2ATaskProjectionStore {
    async fn watch(&self, tenant: &str, task_id: &str) -> TaskProjectionResult<A2ATaskEventSignal> {
        let mut state = self.inner.lock().expect("projection store mutex");
        // Sweep senders whose subscribers have all disconnected so the
        // watcher map stays bounded by live streams, not by every task id
        // ever streamed.
        state
            .watchers
            .retain(|_, sender| sender.receiver_count() > 0);
        let key = (tenant.to_string(), task_id.to_string());
        let receiver = state
            .watchers
            .entry(key)
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(EVENT_WATCH_BUFFER);
                sender
            })
            .subscribe();
        Ok(A2ATaskEventSignal::from_source(Box::new(
            BroadcastSignalSource { receiver },
        )))
    }
}

fn compact_event_log(events: &mut Vec<A2ATaskEvent>, limit: usize) {
    while events.len() > limit {
        let latest_snapshot = events
            .iter()
            .rposition(|event| matches!(event.payload, A2ATaskEventPayload::Snapshot(_)));
        let remove_at = events
            .iter()
            .enumerate()
            .find_map(|(index, event)| {
                if Some(index) != latest_snapshot
                    && !matches!(event.payload, A2ATaskEventPayload::Snapshot(_))
                {
                    Some(index)
                } else {
                    None
                }
            })
            .or_else(|| latest_snapshot.map(|snapshot| snapshot.saturating_sub(1)))
            .unwrap_or(0);
        events.remove(remove_at);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use a2a::{Message, Part, Role, TaskState};

    fn accepted(task_id: &str, revision: u64) -> A2ATaskProjection {
        A2ATaskProjection::accepted(
            task_id,
            "ctx",
            "tenant-a",
            "workflow",
            AgentTimestampMillis::new(10),
            Vec::new(),
            revision,
        )
    }

    #[tokio::test]
    async fn query_store_filters_and_paginates_deterministically() {
        let store = InMemoryA2ATaskProjectionStore::local();
        for index in 0..3 {
            store
                .upsert(A2ATaskProjection::accepted(
                    format!("task-{index}"),
                    "ctx",
                    "tenant-a",
                    "workflow",
                    AgentTimestampMillis::new(index + 1),
                    Vec::new(),
                    index + 1,
                ))
                .await
                .expect("upsert");
        }

        let page1 = store
            .list(&ListTasksRequest {
                context_id: Some("ctx".to_string()),
                status: Some(TaskState::Submitted),
                page_size: Some(2),
                page_token: None,
                history_length: None,
                status_timestamp_after: None,
                include_artifacts: None,
                tenant: Some("tenant-a".to_string()),
            })
            .await
            .expect("page1");
        assert_eq!(page1.tasks.len(), 2);
        assert_eq!(page1.next_page_token, "2");

        let page2 = store
            .list(&ListTasksRequest {
                page_token: Some(page1.next_page_token),
                page_size: Some(2),
                tenant: Some("tenant-a".to_string()),
                context_id: None,
                status: None,
                history_length: None,
                status_timestamp_after: None,
                include_artifacts: None,
            })
            .await
            .expect("page2");
        assert_eq!(page2.tasks.len(), 1);
    }

    #[tokio::test]
    async fn tenant_scoped_store_requires_tenant_filter() {
        let store = InMemoryA2ATaskProjectionStore::tenant_scoped();
        let error = store
            .list(&ListTasksRequest {
                context_id: None,
                status: None,
                page_size: None,
                page_token: None,
                history_length: None,
                status_timestamp_after: None,
                include_artifacts: None,
                tenant: None,
            })
            .await
            .expect_err("tenant required");
        assert_eq!(error.code(), "tenant-required");

        let error = store
            .projection(None, "task-1")
            .await
            .expect_err("unscoped read");
        assert_eq!(error.code(), "tenant-required");
    }

    #[tokio::test]
    async fn failed_event_apply_is_not_recorded_for_replay() {
        let store = InMemoryA2ATaskProjectionStore::local();
        store.upsert(accepted("task-1", 0)).await.expect("upsert");
        let event = A2ATaskEvent::new(
            "tenant-a",
            "task-1",
            "ctx",
            3,
            AgentTimestampMillis::new(20),
            A2ATaskEventPayload::StatusUpdate {
                state: TaskState::Working,
            },
        );

        let error = store.append_event(event).await.expect_err("sequence error");

        assert_eq!(error.code(), "event-order");
        assert!(store
            .replay_events("tenant-a", "task-1", None)
            .await
            .expect("replay")
            .is_empty());
    }

    #[tokio::test]
    async fn replay_cursor_without_local_log_requires_rebootstrap() {
        let store = InMemoryA2ATaskProjectionStore::local();
        // A projection cached from a routed owner response: known revision,
        // but no local event log covering the cursor.
        store.upsert(accepted("task-1", 5)).await.expect("upsert");

        let error = store
            .replay_events("tenant-a", "task-1", Some("task-1:3"))
            .await
            .expect_err("a cursor without a local log must not read as an empty tail");

        assert_eq!(error.code(), "replay-window-expired");
    }

    #[tokio::test]
    async fn replay_cursor_beyond_known_revision_is_invalid() {
        let store = InMemoryA2ATaskProjectionStore::local();
        store
            .append_event_payload(
                "tenant-a",
                "task-1",
                "ctx",
                AgentTimestampMillis::new(10),
                A2ATaskEventPayload::Snapshot(accepted("task-1", 0)),
            )
            .await
            .expect("bootstrap snapshot");

        // A cursor from another node or owner epoch can point past everything
        // recorded here; it cannot prove continuity and must force a resync.
        let error = store
            .replay_events("tenant-a", "task-1", Some("task-1:99"))
            .await
            .expect_err("future cursor must be rejected");

        assert_eq!(error.code(), "invalid-replay-cursor");
    }

    #[tokio::test]
    async fn watch_signals_new_event_sequences_and_prunes_dead_watchers() {
        let store = InMemoryA2ATaskProjectionStore::local();
        store
            .append_event_payload(
                "tenant-a",
                "task-w",
                "ctx",
                AgentTimestampMillis::new(10),
                A2ATaskEventPayload::Snapshot(accepted("task-w", 0)),
            )
            .await
            .expect("bootstrap snapshot");

        let mut signal = store.watch("tenant-a", "task-w").await.expect("watch");
        store
            .append_event_payload(
                "tenant-a",
                "task-w",
                "ctx",
                AgentTimestampMillis::new(11),
                A2ATaskEventPayload::StatusUpdate {
                    state: TaskState::Working,
                },
            )
            .await
            .expect("append");
        assert_eq!(
            signal.changed().await,
            A2ATaskEventSignalOutcome::Notified {
                high_watermark_hint: 2
            }
        );
        drop(signal);

        // Appending to a watcher whose receivers all dropped removes it.
        store
            .append_event_payload(
                "tenant-a",
                "task-w",
                "ctx",
                AgentTimestampMillis::new(12),
                A2ATaskEventPayload::StatusUpdate {
                    state: TaskState::Working,
                },
            )
            .await
            .expect("append after receiver dropped");
        assert_eq!(
            store.watcher_count(),
            0,
            "append must prune watchers with no receivers"
        );

        // Opening any watcher sweeps dead senders left on other tasks.
        let stale = store.watch("tenant-a", "task-w").await.expect("watch");
        drop(stale);
        let _live = store.watch("tenant-a", "task-other").await.expect("watch");
        assert_eq!(store.watcher_count(), 1, "watch must sweep dead senders");
    }

    #[tokio::test]
    async fn orphan_event_for_unknown_task_is_rejected_and_not_recorded() {
        let store = InMemoryA2ATaskProjectionStore::local();
        let event = A2ATaskEvent::new(
            "tenant-a",
            "task-unknown",
            "ctx",
            5,
            AgentTimestampMillis::new(20),
            A2ATaskEventPayload::StatusUpdate {
                state: TaskState::Working,
            },
        );

        let error = store.append_event(event).await.expect_err("orphan event");

        assert_eq!(error.code(), "task-not-found");
        assert!(store
            .replay_events("tenant-a", "task-unknown", None)
            .await
            .expect("replay")
            .is_empty());
    }

    #[tokio::test]
    async fn event_log_is_bounded_per_task() {
        let store = InMemoryA2ATaskProjectionStore::local();
        let limit = DEFAULT_EVENT_LOG_LIMIT;
        store
            .append_event_payload(
                "tenant-a",
                "task-log",
                "ctx",
                AgentTimestampMillis::new(10),
                A2ATaskEventPayload::Snapshot(accepted("task-log", 0)),
            )
            .await
            .expect("bootstrap snapshot");

        for index in 0..limit + 10 {
            let mut message = Message::new(Role::User, vec![Part::text("hello")]);
            message.message_id = format!("msg-{index}");
            store
                .append_event_payload(
                    "tenant-a",
                    "task-log",
                    "ctx",
                    AgentTimestampMillis::new(20 + index as u64),
                    A2ATaskEventPayload::MessageUpdate { message },
                )
                .await
                .expect("append message");
        }

        // Sequences run 1..=LIMIT+11. Compaction keeps the bootstrap snapshot
        // at sequence 1 and the newest live tail, so replay from before the
        // live tail reports the first contiguous retained update.
        let earliest_retained = 13;
        let expired = store
            .replay_events("tenant-a", "task-log", None)
            .await
            .expect_err("replay from before the retained window must fail");
        assert!(matches!(
            expired,
            TaskProjectionError::ReplayWindowExpired {
                earliest_sequence,
                ..
            } if earliest_sequence == earliest_retained
        ));

        let boundary_cursor = format!("task-log:{}", earliest_retained - 1);
        let events = store
            .replay_events("tenant-a", "task-log", Some(&boundary_cursor))
            .await
            .expect("replay from the window boundary");
        assert_eq!(events.len(), limit - 1);
        assert_eq!(
            events.first().map(|event| event.sequence),
            Some(earliest_retained)
        );
    }

    #[tokio::test]
    async fn snapshot_event_bootstraps_unknown_task() {
        let store = InMemoryA2ATaskProjectionStore::local();
        let event = A2ATaskEvent::new(
            "tenant-a",
            "task-boot",
            "ctx",
            1,
            AgentTimestampMillis::new(20),
            A2ATaskEventPayload::Snapshot(accepted("task-boot", 0)),
        );

        store.append_event(event).await.expect("bootstrap snapshot");

        let task = store
            .get(Some("tenant-a"), "task-boot", None)
            .await
            .expect("get");
        assert_eq!(task.id, "task-boot");
        assert_eq!(
            store
                .replay_events("tenant-a", "task-boot", None)
                .await
                .expect("replay")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn replay_cursor_from_another_task_is_rejected() {
        let store = InMemoryA2ATaskProjectionStore::local();
        let error = store
            .replay_events("tenant-a", "task-1", Some("task-2:5"))
            .await
            .expect_err("cursor task mismatch");
        assert_eq!(error.code(), "invalid-replay-cursor");

        let error = store
            .replay_events("tenant-a", "task-1", Some("not-a-cursor"))
            .await
            .expect_err("malformed cursor");
        assert_eq!(error.code(), "invalid-replay-cursor");
    }
}
