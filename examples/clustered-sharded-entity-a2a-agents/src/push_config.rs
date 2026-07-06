//! Durable A2A push notification configuration and outbox scheduling.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use a2a::{
    ListTaskPushNotificationConfigsRequest, ListTaskPushNotificationConfigsResponse,
    TaskPushNotificationConfig,
};
use rakka::agent_workflow::substrate::{WorkflowError, WorkflowState};
use rakka::agent_workflow::{
    AgentAttributes, AgentCausationId, AgentCorrelationId, AgentDeduplicationKey,
    AgentDurabilityMetadata, AgentEffectId, AgentEffectKind, AgentEffectMetadata,
    AgentEffectSchedule, AgentEffectTarget, AgentFacadeError, AgentIdempotencyKey, AgentInboxError,
    AgentOutboxError, AgentRunId, AgentRunInbox, AgentTelemetryContext, AgentTimestampMillis,
};
use rakka::persistence::{DurableError, DurableStateStore, PersistenceId, Revision};
use serde::{Deserialize, Serialize};

use crate::durable_stores::PushConfigStore;
use crate::support::{current_timestamp_millis, hex_encode};
use crate::task_projection::{
    encode_replay_cursor, A2ATaskEvent, A2ATaskEventRedaction, InMemoryA2ATaskProjectionStore,
};

const PUSH_CONFIG_PERSISTENCE_PREFIX: &str = "a2a-push-config";
const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 100;
/// Bound on retained audit records per push config; request-level configs
/// are re-saved on every send, so the trail must not grow with traffic.
const MAX_PUSH_CONFIG_AUDIT_RECORDS: usize = 32;
static GENERATED_CONFIG_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) type A2APushConfigResult<T> = Result<T, A2APushConfigError>;

#[derive(Debug, Clone)]
pub(crate) enum A2APushConfigError {
    InvalidConfig {
        field: &'static str,
        reason: &'static str,
    },
    InvalidPageToken {
        token: String,
    },
    ConfigNotFound {
        task_id: String,
        config_id: String,
    },
    Persistence(DurableError),
    Facade(AgentFacadeError),
    Inbox(AgentInboxError),
    Outbox(AgentOutboxError),
}

impl A2APushConfigError {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::InvalidConfig { .. } => "invalid-push-config",
            Self::InvalidPageToken { .. } => "invalid-page-token",
            Self::ConfigNotFound { .. } => "push-config-not-found",
            Self::Persistence(error) => error.code(),
            Self::Facade(error) => facade_error_code(error),
            Self::Inbox(error) => error.code(),
            Self::Outbox(error) => error.code(),
        }
    }
}

impl Display for A2APushConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig { field, reason } => {
                write!(f, "invalid push config {field}: {reason}")
            }
            Self::InvalidPageToken { token } => write!(f, "invalid page token `{token}`"),
            Self::ConfigNotFound { task_id, config_id } => {
                write!(f, "push config {config_id} not found for task {task_id}")
            }
            Self::Persistence(error) => Display::fmt(error, f),
            Self::Facade(error) => Display::fmt(error, f),
            Self::Inbox(error) => Display::fmt(error, f),
            Self::Outbox(error) => Display::fmt(error, f),
        }
    }
}

impl Error for A2APushConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Persistence(error) => Some(error),
            Self::Facade(error) => Some(error),
            Self::Inbox(error) => Some(error),
            Self::Outbox(error) => Some(error),
            Self::InvalidConfig { .. }
            | Self::InvalidPageToken { .. }
            | Self::ConfigNotFound { .. } => None,
        }
    }
}

impl From<DurableError> for A2APushConfigError {
    fn from(error: DurableError) -> Self {
        Self::Persistence(error)
    }
}

impl From<AgentFacadeError> for A2APushConfigError {
    fn from(error: AgentFacadeError) -> Self {
        Self::Facade(error)
    }
}

impl From<AgentInboxError> for A2APushConfigError {
    fn from(error: AgentInboxError) -> Self {
        Self::Inbox(error)
    }
}

impl From<AgentOutboxError> for A2APushConfigError {
    fn from(error: AgentOutboxError) -> Self {
        Self::Outbox(error)
    }
}

fn facade_error_code(error: &AgentFacadeError) -> &'static str {
    match error {
        AgentFacadeError::InvalidCommandMetadata { .. } => "invalid-command-metadata",
        AgentFacadeError::InvalidCommand { .. } => "invalid-command",
        AgentFacadeError::InvalidEffectMetadata { .. } => "invalid-effect-metadata",
        AgentFacadeError::InvalidEffect { .. } => "invalid-effect",
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct A2APushConfigState {
    pub(crate) tenant: String,
    pub(crate) task_id: String,
    pub(crate) config_id: String,
    pub(crate) config: TaskPushNotificationConfig,
    pub(crate) auth: A2APushConfigAuthMetadata,
    pub(crate) deleted: bool,
    pub(crate) created_at: AgentTimestampMillis,
    pub(crate) updated_at: AgentTimestampMillis,
    pub(crate) audit: Vec<A2APushConfigAuditRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct A2APushConfigAuthMetadata {
    pub(crate) token_present: bool,
    pub(crate) authentication_scheme: Option<String>,
    pub(crate) credentials_present: bool,
    pub(crate) redaction: A2ATaskEventRedaction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct A2APushConfigAuditRecord {
    pub(crate) kind: A2APushConfigAuditKind,
    pub(crate) occurred_at: AgentTimestampMillis,
    pub(crate) redaction: A2ATaskEventRedaction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum A2APushConfigAuditKind {
    Created,
    Updated,
    Deleted,
}

#[derive(Debug, Clone)]
pub(crate) struct A2APushConfigStore {
    store: PushConfigStore,
    /// Highest event sequence per (tenant, task id) whose push effects were
    /// durably scheduled (or confirmed unnecessary). Node-local memory: on
    /// loss the retained event log is re-offered and the durable
    /// deduplication keys drop anything scheduled twice.
    scheduled: Arc<Mutex<BTreeMap<(String, String), u64>>>,
}

impl A2APushConfigStore {
    pub(crate) fn new(store: PushConfigStore) -> Self {
        Self {
            store,
            scheduled: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn scheduled_watermark(&self, tenant: &str, task_id: &str) -> u64 {
        self.scheduled
            .lock()
            .expect("push watermark mutex")
            .get(&(tenant.to_string(), task_id.to_string()))
            .copied()
            .unwrap_or(0)
    }

    /// Overwrites (rather than maxes) the watermark: event sequences restart
    /// per owner epoch, so tracking the current epoch's tail is correct and
    /// a stale higher value would skip real events forever.
    fn record_scheduled_watermark(&self, tenant: &str, task_id: &str, sequence: u64) {
        self.scheduled
            .lock()
            .expect("push watermark mutex")
            .insert((tenant.to_string(), task_id.to_string()), sequence);
    }

    pub(crate) async fn save(
        &self,
        tenant: &str,
        mut config: TaskPushNotificationConfig,
    ) -> A2APushConfigResult<TaskPushNotificationConfig> {
        validate_tenant(tenant)?;
        if config
            .tenant
            .as_deref()
            .is_some_and(|value| value != tenant)
        {
            return Err(A2APushConfigError::InvalidConfig {
                field: "tenant",
                reason: "tenant does not match authenticated tenant",
            });
        }
        config.tenant = Some(tenant.to_string());
        validate_config(&config)?;

        if config.id.as_deref().is_none_or(str::is_empty) {
            config.id = Some(generated_config_id());
        }
        let config_id = config.id.clone().expect("config id set");
        let persistence_id = push_config_persistence_id(tenant, &config.task_id, &config_id);
        let now = AgentTimestampMillis::new(current_timestamp_millis());
        let auth = auth_metadata(&config);
        let redacted = redacted_config(config);

        let existing = self.store.load(&persistence_id).await?;
        let (expected, state) = match existing {
            Some(record) => {
                let mut state = record.state;
                // Request-level configs arrive on every send; an identical
                // re-save is a no-op read rather than a durable rewrite with
                // audit churn.
                if !state.deleted && state.config == redacted && state.auth == auth {
                    return Ok(state.config);
                }
                let kind = if state.deleted {
                    A2APushConfigAuditKind::Created
                } else {
                    A2APushConfigAuditKind::Updated
                };
                state.config = redacted.clone();
                state.auth = auth.clone();
                state.deleted = false;
                state.updated_at = now;
                state.audit.push(A2APushConfigAuditRecord {
                    kind,
                    occurred_at: now,
                    redaction: auth.redaction,
                });
                cap_audit(&mut state.audit);
                (record.revision, state)
            }
            None => (
                Revision::INITIAL,
                A2APushConfigState {
                    tenant: tenant.to_string(),
                    task_id: redacted.task_id.clone(),
                    config_id,
                    config: redacted.clone(),
                    auth: auth.clone(),
                    deleted: false,
                    created_at: now,
                    updated_at: now,
                    audit: vec![A2APushConfigAuditRecord {
                        kind: A2APushConfigAuditKind::Created,
                        occurred_at: now,
                        redaction: auth.redaction,
                    }],
                },
            ),
        };
        let record = self
            .store
            .compare_and_set(&persistence_id, expected, state)
            .await?;
        Ok(record.state.config)
    }

    pub(crate) async fn get(
        &self,
        tenant: &str,
        task_id: &str,
        config_id: &str,
    ) -> A2APushConfigResult<TaskPushNotificationConfig> {
        validate_tenant(tenant)?;
        let state = self
            .store
            .load(&push_config_persistence_id(tenant, task_id, config_id))
            .await?
            .map(|record| record.state)
            .filter(|state| !state.deleted)
            .ok_or_else(|| A2APushConfigError::ConfigNotFound {
                task_id: task_id.to_string(),
                config_id: config_id.to_string(),
            })?;
        Ok(state.config)
    }

    pub(crate) async fn list(
        &self,
        tenant: &str,
        request: &ListTaskPushNotificationConfigsRequest,
    ) -> A2APushConfigResult<ListTaskPushNotificationConfigsResponse> {
        validate_tenant(tenant)?;
        let offset = page_offset(request.page_token.as_deref())?;
        let page_size = page_size(request.page_size);
        let mut states = self.active_states(tenant, &request.task_id).await?;
        states.sort_by(|left, right| left.config_id.cmp(&right.config_id));

        let configs = states
            .iter()
            .skip(offset)
            .take(page_size)
            .map(|state| state.config.clone())
            .collect::<Vec<_>>();
        let next_offset = offset.saturating_add(configs.len());
        let next_page_token = (next_offset < states.len()).then(|| next_offset.to_string());
        Ok(ListTaskPushNotificationConfigsResponse {
            configs,
            next_page_token,
        })
    }

    pub(crate) async fn delete(
        &self,
        tenant: &str,
        task_id: &str,
        config_id: &str,
    ) -> A2APushConfigResult<()> {
        validate_tenant(tenant)?;
        let persistence_id = push_config_persistence_id(tenant, task_id, config_id);
        let Some(record) = self.store.load(&persistence_id).await? else {
            return Ok(());
        };
        if record.state.deleted {
            return Ok(());
        }
        let now = AgentTimestampMillis::new(current_timestamp_millis());
        let mut state = record.state;
        state.deleted = true;
        state.updated_at = now;
        state.audit.push(A2APushConfigAuditRecord {
            kind: A2APushConfigAuditKind::Deleted,
            occurred_at: now,
            redaction: state.auth.redaction,
        });
        cap_audit(&mut state.audit);
        self.store
            .compare_and_set(&persistence_id, record.revision, state)
            .await?;
        Ok(())
    }

    pub(crate) async fn active_configs(
        &self,
        tenant: &str,
        task_id: &str,
    ) -> A2APushConfigResult<Vec<TaskPushNotificationConfig>> {
        Ok(self
            .active_states(tenant, task_id)
            .await?
            .into_iter()
            .map(|state| state.config)
            .collect())
    }

    async fn active_states(
        &self,
        tenant: &str,
        task_id: &str,
    ) -> A2APushConfigResult<Vec<A2APushConfigState>> {
        let prefix = push_config_persistence_prefix(tenant, task_id);
        let mut states = Vec::new();
        for id in self.store.persistence_ids().await? {
            if !id.as_str().starts_with(&prefix) {
                continue;
            }
            if let Some(record) = self.store.load(&id).await? {
                if !record.state.deleted {
                    states.push(record.state);
                }
            }
        }
        Ok(states)
    }
}

/// Bound on optimistic-concurrency re-drives for push-effect scheduling.
/// The run actor can advance the same durable workflow state between the
/// batch's recovery and its schedule writes; each retry requires a distinct
/// concurrent writer, so the bound is a livelock guard.
const MAX_PUSH_SCHEDULE_REDRIVES: usize = 3;

/// Schedules push notification effects for a task's public events.
///
/// The unit of work is the task's event log past the store's scheduled
/// watermark, not just `newly_emitted`: a scheduling failure on an earlier
/// request leaves the watermark behind, so the client's retry (or any later
/// read that converges the projection) re-offers the missed events and heals
/// the gap. The watermark only advances on success, and the idempotency keys
/// derived from task id, event sequence, and config id make re-offered
/// events deduplicate instead of double-scheduling.
///
/// One config scan and one workflow inbox recovery serve the whole batch,
/// and revision conflicts with the run actor's own inbox writes are
/// re-driven a bounded number of times.
pub(crate) async fn schedule_push_effects_for_events<WorkflowStoreT>(
    workflow_store: &WorkflowStoreT,
    push_configs: &A2APushConfigStore,
    task_store: &InMemoryA2ATaskProjectionStore,
    tenant: &str,
    task_id: &str,
    newly_emitted: &[A2ATaskEvent],
) -> A2APushConfigResult<usize>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    let watermark = push_configs.scheduled_watermark(tenant, task_id);
    let cursor = encode_replay_cursor(task_id, watermark);
    let pending = match task_store.replay_events(tenant, task_id, Some(&cursor)) {
        Ok(events) => events,
        // The log cannot resume from the watermark (compaction, or an owner
        // epoch change reset the sequences). Events older than the retained
        // window are past this example's retention policy; schedule what the
        // caller just emitted and re-anchor the watermark to it.
        Err(_) => newly_emitted.to_vec(),
    };
    let Some(last_sequence) = pending.last().map(|event| event.sequence) else {
        return Ok(0);
    };

    let configs = push_configs.active_configs(tenant, task_id).await?;
    if configs.is_empty() {
        // Configs apply from registration onward; mark these events handled
        // so a config added later does not receive historical events.
        push_configs.record_scheduled_watermark(tenant, task_id, last_sequence);
        return Ok(0);
    }

    let mut attempts = 0;
    loop {
        match schedule_effect_batch(workflow_store, task_id, &configs, &pending).await {
            Ok(scheduled) => {
                push_configs.record_scheduled_watermark(tenant, task_id, last_sequence);
                return Ok(scheduled);
            }
            Err(error)
                if attempts < MAX_PUSH_SCHEDULE_REDRIVES && schedule_revision_conflict(&error) =>
            {
                attempts += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn schedule_effect_batch<WorkflowStoreT>(
    workflow_store: &WorkflowStoreT,
    task_id: &str,
    configs: &[TaskPushNotificationConfig],
    events: &[A2ATaskEvent],
) -> A2APushConfigResult<usize>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    let mut inbox =
        AgentRunInbox::new(AgentRunId::new(task_id.to_string()), workflow_store.clone());
    inbox.recover().await?;

    let mut scheduled = 0;
    for event in events {
        for config in configs {
            let effect = push_effect(event, config)?;
            inbox.schedule_effect(effect).await?;
            scheduled += 1;
        }
    }
    Ok(scheduled)
}

fn schedule_revision_conflict(error: &A2APushConfigError) -> bool {
    matches!(
        error,
        A2APushConfigError::Outbox(AgentOutboxError::Workflow {
            error: WorkflowError::RevisionConflict { .. },
        })
    )
}

fn push_effect(
    event: &A2ATaskEvent,
    config: &TaskPushNotificationConfig,
) -> A2APushConfigResult<rakka::agent_workflow::AgentEffect> {
    let config_id = config.id.as_deref().unwrap_or("default");
    let key = format!(
        "a2a-push:{}:{}:{}",
        event.task_id, event.sequence, config_id
    );
    let durability = AgentDurabilityMetadata::new(
        AgentDeduplicationKey::new(key.clone()),
        AgentCausationId::new(format!(
            "a2a-task-event:{}:{}",
            event.task_id, event.sequence
        )),
        AgentCorrelationId::new(format!("a2a-task:{}", event.task_id)),
    )
    .telemetry_context(AgentTelemetryContext::default());
    let metadata = AgentEffectMetadata::new(
        AgentEffectId::new(key.clone()),
        durability,
        AgentIdempotencyKey::new(key),
        event.occurred_at,
    )?
    .timeout_ms(30_000);

    let mut attributes = AgentAttributes::new();
    attributes.insert("notification_protocol".to_string(), "a2a-push".to_string());
    attributes.insert(
        "task_event_kind".to_string(),
        event.kind().as_label().to_string(),
    );
    attributes.insert(
        "task_state".to_string(),
        task_state_label(&event.projected_state).to_string(),
    );
    attributes.insert(
        "redaction".to_string(),
        event.redaction.as_label().to_string(),
    );

    let target = AgentEffectTarget {
        target_type: "notification".to_string(),
        name: "a2a-push-webhook".to_string(),
        address: Some(config.url.clone()),
        attributes,
    };
    let schedule = AgentEffectSchedule::new(AgentEffectKind::Notification, target, metadata)?
        .expected_result_type("a2a.push.delivery")?;
    Ok(schedule.into_effect()?)
}

/// Keeps the newest `MAX_PUSH_CONFIG_AUDIT_RECORDS` audit entries.
fn cap_audit(audit: &mut Vec<A2APushConfigAuditRecord>) {
    if audit.len() > MAX_PUSH_CONFIG_AUDIT_RECORDS {
        let excess = audit.len() - MAX_PUSH_CONFIG_AUDIT_RECORDS;
        audit.drain(..excess);
    }
}

fn redacted_config(mut config: TaskPushNotificationConfig) -> TaskPushNotificationConfig {
    config.token = None;
    if let Some(authentication) = config.authentication.as_mut() {
        authentication.credentials = None;
    }
    config
}

fn auth_metadata(config: &TaskPushNotificationConfig) -> A2APushConfigAuthMetadata {
    A2APushConfigAuthMetadata {
        token_present: config.token.is_some(),
        authentication_scheme: config
            .authentication
            .as_ref()
            .map(|authentication| authentication.scheme.clone()),
        credentials_present: config
            .authentication
            .as_ref()
            .and_then(|authentication| authentication.credentials.as_ref())
            .is_some(),
        redaction: A2ATaskEventRedaction::Redacted,
    }
}

fn validate_config(config: &TaskPushNotificationConfig) -> A2APushConfigResult<()> {
    if config.task_id.trim().is_empty() {
        return Err(A2APushConfigError::InvalidConfig {
            field: "task_id",
            reason: "task id is required",
        });
    }
    if config.url.trim().is_empty() {
        return Err(A2APushConfigError::InvalidConfig {
            field: "url",
            reason: "callback URL is required",
        });
    }
    if !valid_callback_url(&config.url) {
        return Err(A2APushConfigError::InvalidConfig {
            field: "url",
            reason: "callback URL must be an absolute HTTP or HTTPS URL without userinfo",
        });
    }
    if config.id.as_deref().is_some_and(|id| id.trim().is_empty()) {
        return Err(A2APushConfigError::InvalidConfig {
            field: "id",
            reason: "config id must not be blank",
        });
    }
    if let Some(authentication) = &config.authentication {
        if authentication.scheme.trim().is_empty() {
            return Err(A2APushConfigError::InvalidConfig {
                field: "authentication.scheme",
                reason: "authentication scheme must not be blank",
            });
        }
    }
    Ok(())
}

fn validate_tenant(tenant: &str) -> A2APushConfigResult<()> {
    if tenant.trim().is_empty() {
        return Err(A2APushConfigError::InvalidConfig {
            field: "tenant",
            reason: "tenant is required",
        });
    }
    Ok(())
}

fn valid_callback_url(url: &str) -> bool {
    let Some(rest) = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
    else {
        return false;
    };
    !rest.is_empty() && !rest.contains('@') && !rest.contains(char::is_whitespace)
}

fn generated_config_id() -> String {
    format!(
        "cfg-{}-{}",
        current_timestamp_millis(),
        GENERATED_CONFIG_COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

fn push_config_persistence_prefix(tenant: &str, task_id: &str) -> String {
    format!(
        "{PUSH_CONFIG_PERSISTENCE_PREFIX}:{}:{}:",
        hex_encode(tenant),
        hex_encode(task_id)
    )
}

fn push_config_persistence_id(tenant: &str, task_id: &str, config_id: &str) -> PersistenceId {
    PersistenceId::new(format!(
        "{}{}",
        push_config_persistence_prefix(tenant, task_id),
        hex_encode(config_id)
    ))
}

fn page_offset(page_token: Option<&str>) -> A2APushConfigResult<usize> {
    match page_token {
        None | Some("") => Ok(0),
        Some(token) => token
            .parse::<usize>()
            .map_err(|_| A2APushConfigError::InvalidPageToken {
                token: token.to_string(),
            }),
    }
}

fn page_size(page_size: Option<i32>) -> usize {
    match page_size {
        Some(value) if value > 0 => usize::try_from(value).unwrap_or(DEFAULT_PAGE_SIZE),
        _ => DEFAULT_PAGE_SIZE,
    }
    .min(MAX_PAGE_SIZE)
}

fn task_state_label(state: &a2a::TaskState) -> &'static str {
    match state {
        a2a::TaskState::Unspecified => "unspecified",
        a2a::TaskState::Submitted => "submitted",
        a2a::TaskState::Working => "working",
        a2a::TaskState::Completed => "completed",
        a2a::TaskState::Failed => "failed",
        a2a::TaskState::Canceled => "canceled",
        a2a::TaskState::InputRequired => "input-required",
        a2a::TaskState::Rejected => "rejected",
        a2a::TaskState::AuthRequired => "auth-required",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable_stores::PushConfigStore;

    fn store() -> A2APushConfigStore {
        A2APushConfigStore::new(PushConfigStore::memory())
    }

    fn config(task_id: &str, id: &str) -> TaskPushNotificationConfig {
        TaskPushNotificationConfig {
            url: "https://example.com/hook".to_string(),
            id: Some(id.to_string()),
            task_id: task_id.to_string(),
            token: Some("secret-token".to_string()),
            authentication: Some(a2a::AuthenticationInfo {
                scheme: "bearer".to_string(),
                credentials: Some("secret".to_string()),
            }),
            tenant: None,
        }
    }

    #[tokio::test]
    async fn save_get_list_and_delete_redacts_auth_material() {
        let store = store();
        let saved = store
            .save("tenant-a", config("task-1", "cfg-1"))
            .await
            .expect("save");
        assert_eq!(saved.id.as_deref(), Some("cfg-1"));
        assert!(saved.token.is_none());
        assert!(saved
            .authentication
            .as_ref()
            .and_then(|auth| auth.credentials.as_ref())
            .is_none());

        let fetched = store.get("tenant-a", "task-1", "cfg-1").await.expect("get");
        assert_eq!(fetched.url, "https://example.com/hook");
        let persisted = store
            .store
            .load(&push_config_persistence_id("tenant-a", "task-1", "cfg-1"))
            .await
            .expect("load persisted")
            .expect("persisted state")
            .state;
        assert!(persisted.auth.token_present);
        assert!(persisted.auth.credentials_present);
        assert!(persisted.config.token.is_none());

        let listed = store
            .list(
                "tenant-a",
                &ListTaskPushNotificationConfigsRequest {
                    task_id: "task-1".to_string(),
                    page_size: None,
                    page_token: None,
                    tenant: Some("tenant-a".to_string()),
                },
            )
            .await
            .expect("list");
        assert_eq!(listed.configs.len(), 1);

        store
            .delete("tenant-a", "task-1", "cfg-1")
            .await
            .expect("delete");
        assert!(store.get("tenant-a", "task-1", "cfg-1").await.is_err());
        assert!(store
            .active_configs("tenant-a", "task-1")
            .await
            .expect("active")
            .is_empty());
    }

    #[tokio::test]
    async fn invalid_callback_url_is_rejected() {
        let store = store();
        let mut config = config("task-1", "cfg-1");
        config.url = "file:///tmp/hook".to_string();
        let error = store.save("tenant-a", config).await.expect_err("invalid");
        assert_eq!(error.code(), "invalid-push-config");
    }

    #[tokio::test]
    async fn identical_resave_skips_audit_and_audit_stays_bounded() {
        let store = store();
        store
            .save("tenant-a", config("task-1", "cfg-1"))
            .await
            .expect("save");
        // Request-level configs arrive on every send; an identical re-save
        // must not grow the durable record.
        store
            .save("tenant-a", config("task-1", "cfg-1"))
            .await
            .expect("re-save");
        let persisted = store
            .store
            .load(&push_config_persistence_id("tenant-a", "task-1", "cfg-1"))
            .await
            .expect("load")
            .expect("state")
            .state;
        assert_eq!(
            persisted.audit.len(),
            1,
            "identical re-save must not append audit records"
        );

        // Genuine updates keep the audit trail bounded.
        for index in 0..(MAX_PUSH_CONFIG_AUDIT_RECORDS + 8) {
            let mut updated = config("task-1", "cfg-1");
            updated.url = format!("https://example.com/hook-{index}");
            store.save("tenant-a", updated).await.expect("update");
        }
        let persisted = store
            .store
            .load(&push_config_persistence_id("tenant-a", "task-1", "cfg-1"))
            .await
            .expect("load")
            .expect("state")
            .state;
        assert!(
            persisted.audit.len() <= MAX_PUSH_CONFIG_AUDIT_RECORDS,
            "audit trail must stay bounded, got {}",
            persisted.audit.len()
        );
    }

    #[tokio::test]
    async fn watermark_catchup_heals_missed_schedules_and_deduplicates() {
        use crate::durable_stores::WorkflowStore;
        use crate::task_projection::{A2ATaskEventPayload, A2ATaskProjection};

        let store = store();
        store
            .save("tenant-a", config("task-1", "cfg-1"))
            .await
            .expect("save config");
        let workflow_store = WorkflowStore::memory();
        let task_store = InMemoryA2ATaskProjectionStore::local();

        // Nothing recorded yet: an empty batch is a no-op.
        assert_eq!(
            schedule_push_effects_for_events(
                &workflow_store,
                &store,
                &task_store,
                "tenant-a",
                "task-1",
                &[],
            )
            .await
            .expect("empty task"),
            0
        );

        // Two events land in the log as if a prior request appended them but
        // its scheduling failed before advancing the watermark.
        let snapshot = A2ATaskProjection::accepted(
            "task-1",
            "ctx",
            "tenant-a",
            "workflow",
            AgentTimestampMillis::new(10),
            Vec::new(),
            0,
        );
        task_store
            .append_event_payload(
                "tenant-a",
                "task-1",
                "ctx",
                AgentTimestampMillis::new(10),
                A2ATaskEventPayload::Snapshot(snapshot),
            )
            .expect("snapshot event");
        task_store
            .append_event_payload(
                "tenant-a",
                "task-1",
                "ctx",
                AgentTimestampMillis::new(11),
                A2ATaskEventPayload::StatusUpdate {
                    state: a2a::TaskState::Working,
                },
            )
            .expect("status event");

        // A later call with nothing newly emitted (a client retry or a read)
        // heals the gap from the retained log.
        let caught_up = schedule_push_effects_for_events(
            &workflow_store,
            &store,
            &task_store,
            "tenant-a",
            "task-1",
            &[],
        )
        .await
        .expect("catch up");
        assert_eq!(caught_up, 2, "missed events must be scheduled by catch-up");

        // The watermark now covers the log: repeating is a fast no-op.
        assert_eq!(
            schedule_push_effects_for_events(
                &workflow_store,
                &store,
                &task_store,
                "tenant-a",
                "task-1",
                &[],
            )
            .await
            .expect("no-op"),
            0
        );

        // A new event goes through the normal path and only it is offered.
        let terminal = task_store
            .append_event_payload(
                "tenant-a",
                "task-1",
                "ctx",
                AgentTimestampMillis::new(12),
                A2ATaskEventPayload::Terminal {
                    state: a2a::TaskState::Completed,
                },
            )
            .expect("terminal event");
        let scheduled = schedule_push_effects_for_events(
            &workflow_store,
            &store,
            &task_store,
            "tenant-a",
            "task-1",
            &[terminal],
        )
        .await
        .expect("schedule terminal");
        assert_eq!(scheduled, 1);

        // A lost watermark (process restart) re-offers the whole retained
        // log; the durable deduplication keys drop the repeats.
        store.record_scheduled_watermark("tenant-a", "task-1", 0);
        schedule_push_effects_for_events(
            &workflow_store,
            &store,
            &task_store,
            "tenant-a",
            "task-1",
            &[],
        )
        .await
        .expect("re-offer after watermark loss");
        let mut inbox = AgentRunInbox::new(AgentRunId::new("task-1"), workflow_store);
        inbox.recover().await.expect("recover");
        let due = inbox.due_effects().expect("due effects");
        assert_eq!(due.len(), 3, "re-offered events must not duplicate effects");
    }

    #[test]
    fn only_revision_conflicts_are_redriven() {
        use rakka::agent_workflow::substrate::WorkflowId;
        use rakka::persistence::Revision;

        let conflict = A2APushConfigError::Outbox(AgentOutboxError::Workflow {
            error: WorkflowError::RevisionConflict {
                workflow_id: WorkflowId::new("task-1"),
                expected: Revision::INITIAL,
                actual: Revision::INITIAL,
            },
        });
        assert!(schedule_revision_conflict(&conflict));

        let other = A2APushConfigError::InvalidPageToken {
            token: "x".to_string(),
        };
        assert!(!schedule_revision_conflict(&other));
    }
}
