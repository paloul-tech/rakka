//! Durable A2A push notification configuration and outbox scheduling.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};

use a2a::{
    ListTaskPushNotificationConfigsRequest, ListTaskPushNotificationConfigsResponse,
    TaskPushNotificationConfig,
};
use rakka::agent_workflow::substrate::WorkflowState;
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
use crate::task_projection::{A2ATaskEvent, A2ATaskEventRedaction};

const PUSH_CONFIG_PERSISTENCE_PREFIX: &str = "a2a-push-config";
const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 100;
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
}

impl A2APushConfigStore {
    pub(crate) fn new(store: PushConfigStore) -> Self {
        Self { store }
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

pub(crate) async fn schedule_push_effects_for_event<WorkflowStoreT>(
    workflow_store: &WorkflowStoreT,
    push_configs: &A2APushConfigStore,
    event: &A2ATaskEvent,
) -> A2APushConfigResult<usize>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    let configs = push_configs
        .active_configs(&event.tenant, &event.task_id)
        .await?;
    if configs.is_empty() {
        return Ok(0);
    }

    let mut inbox = AgentRunInbox::new(
        AgentRunId::new(event.task_id.clone()),
        workflow_store.clone(),
    );
    inbox.recover().await?;

    let mut scheduled = 0;
    for config in configs {
        let effect = push_effect(event, &config)?;
        inbox.schedule_effect(effect).await?;
        scheduled += 1;
    }
    Ok(scheduled)
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
}
