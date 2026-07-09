//! Durable A2A push notification configuration and outbox scheduling.
//!
//! Push configs are stored with credentials redacted: the crate never
//! persists resolved credentials or secret material in state, outbox
//! effects, task events, logs, metrics, snapshots, or indexes. By default
//! raw credential material is **rejected**; applications that own secret
//! storage supply an [`A2ACredentialBindingResolver`] whose logical binding
//! reference is the only credential-related value persisted.
//!
//! Push delivery is at-least-once: effects carry stable idempotency keys
//! derived from task id, event sequence, and config id, so the webhook
//! target must deduplicate if it needs exactly-once semantics.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use a2a::{
    ListTaskPushNotificationConfigsRequest, ListTaskPushNotificationConfigsResponse,
    TaskPushNotificationConfig,
};
use rakka_agent_workflow::substrate::{WorkflowError, WorkflowState};
use rakka_agent_workflow::{
    AgentAttributes, AgentCausationId, AgentCorrelationId, AgentDeduplicationKey,
    AgentDurabilityMetadata, AgentEffect, AgentEffectId, AgentEffectKind, AgentEffectMetadata,
    AgentEffectSchedule, AgentEffectTarget, AgentFacadeError, AgentIdempotencyKey, AgentInboxError,
    AgentOutboxError, AgentRunId, AgentRunInbox, AgentTelemetryContext, AgentTimestampMillis,
};
use rakka_persistence::{DurableError, DurableStateStore, PersistenceId, Revision};
use serde::{Deserialize, Serialize};

use crate::stores::SharedDurableStateStore;
use crate::support::{current_timestamp_millis, hex_encode};
use crate::task::{encode_replay_cursor, A2ATaskEvent, A2ATaskEventRedaction};

const PUSH_CONFIG_PERSISTENCE_PREFIX: &str = "a2a-push-config";
const DEFAULT_PAGE_SIZE: usize = 50;
const MAX_PAGE_SIZE: usize = 100;
/// Bound on retained audit records per push config; request-level configs
/// are re-saved on every send, so the trail must not grow with traffic.
const MAX_PUSH_CONFIG_AUDIT_RECORDS: usize = 32;
/// Bound on retained push configs per task. Request-level configs can arrive
/// with fresh generated ids, so the per-task aggregate record must not grow
/// without limit; the oldest soft-deleted (then oldest live) entry is dropped.
const MAX_PUSH_CONFIGS_PER_TASK: usize = 64;
/// Bound on optimistic-concurrency re-drives when writing the shared per-task
/// record; each retry needs a distinct concurrent writer, so this is a
/// livelock guard, not a backoff.
const MAX_PUSH_CONFIG_WRITE_ATTEMPTS: usize = 5;
static GENERATED_CONFIG_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Shared durable store handle for push config state.
pub type A2APushConfigStateStore = SharedDurableStateStore<A2APushConfigState>;

/// Shared result type for push configuration.
pub type A2APushConfigResult<T> = Result<T, A2APushConfigError>;

/// Stable push configuration failures.
#[derive(Debug, Clone)]
pub enum A2APushConfigError {
    /// A push config field failed validation or policy.
    InvalidConfig {
        /// Stable field name.
        field: &'static str,
        /// Stable reason.
        reason: &'static str,
    },
    /// Page token is malformed.
    InvalidPageToken {
        /// Token supplied by the client.
        token: String,
    },
    /// The requested push config does not exist.
    ConfigNotFound {
        /// Task id.
        task_id: String,
        /// Config id.
        config_id: String,
    },
    /// Durable store failure.
    Persistence(DurableError),
    /// Agent facade validation failure.
    Facade(AgentFacadeError),
    /// Durable inbox failure.
    Inbox(AgentInboxError),
    /// Durable outbox failure.
    Outbox(AgentOutboxError),
}

impl A2APushConfigError {
    /// Stable machine-readable code.
    #[must_use]
    pub fn code(&self) -> &'static str {
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

/// Logical reference to an application-managed credential binding.
///
/// Only this reference is persisted; the application backend resolves it to
/// real credentials at dispatch time. The value must never contain secret
/// material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct A2ACredentialBindingRef(String);

impl A2ACredentialBindingRef {
    /// Creates a binding reference.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The stable reference value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Resolves raw push credential input to a logical binding reference.
///
/// Implemented by the application backend that owns secret storage. The
/// resolver may inspect the incoming config (including transient credential
/// fields) to mint or look up a binding; `rakka-a2a` persists only the
/// returned reference and never the raw values.
pub trait A2ACredentialBindingResolver: Send + Sync + 'static {
    /// Resolves the binding for a saved config; `Ok(None)` means the config
    /// carries no credential binding.
    fn resolve(
        &self,
        tenant: &str,
        config: &TaskPushNotificationConfig,
    ) -> A2APushConfigResult<Option<A2ACredentialBindingRef>>;
}

/// Policy for push configs that arrive carrying raw credential material.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum A2APushCredentialPolicy {
    /// Reject configs carrying raw credentials unless a binding resolver is
    /// configured (secure default).
    #[default]
    RejectRawCredentials,
    /// Strip raw credentials and record only their presence. Appropriate for
    /// local/demo deployments without a secret backend; deliveries then go
    /// out unauthenticated.
    RedactAndRecordPresence,
}

/// Durable push-config record for one `(tenant, task_id)` scope.
///
/// Holds every push config registered for the task as bounded entries, so a
/// single durable `load` serves scheduling, reads, and listing without
/// scanning the store. Public so applications can wire their chosen
/// durable-state backend (for example a PostgreSQL store with a JSON codec)
/// for push config storage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct A2APushConfigState {
    /// Owning tenant.
    pub tenant: String,
    /// Task id.
    pub task_id: String,
    /// Registered push configs, one entry per config id. Soft-deleted entries
    /// are retained until compacted, and the total is bounded per task.
    pub configs: Vec<A2APushConfigEntry>,
}

/// One redacted push config within a task's durable record.
///
/// The stored config is always redacted; `auth` carries only presence
/// metadata and the optional logical binding reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct A2APushConfigEntry {
    /// Config id.
    pub config_id: String,
    /// Redacted public config (no token, no credentials).
    pub config: TaskPushNotificationConfig,
    /// Redacted credential-presence metadata.
    pub auth: A2APushConfigAuthMetadata,
    /// Soft-delete flag.
    pub deleted: bool,
    /// Creation timestamp.
    pub created_at: AgentTimestampMillis,
    /// Last update timestamp.
    pub updated_at: AgentTimestampMillis,
    /// Bounded audit tail.
    pub audit: Vec<A2APushConfigAuditRecord>,
}

/// Redacted credential-presence metadata for one push config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct A2APushConfigAuthMetadata {
    /// Whether the client supplied a token (never stored).
    pub token_present: bool,
    /// Authentication scheme label, when supplied.
    pub authentication_scheme: Option<String>,
    /// Whether the client supplied credentials (never stored).
    pub credentials_present: bool,
    /// Redaction marker for this metadata.
    pub redaction: A2ATaskEventRedaction,
    /// Logical credential binding reference, when resolved.
    #[serde(default)]
    pub credential_binding_ref: Option<A2ACredentialBindingRef>,
}

/// One bounded audit record for a push config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct A2APushConfigAuditRecord {
    /// Change kind.
    pub kind: A2APushConfigAuditKind,
    /// Change timestamp.
    pub occurred_at: AgentTimestampMillis,
    /// Redaction marker for the audited change.
    pub redaction: A2ATaskEventRedaction,
}

/// Push config audit change kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum A2APushConfigAuditKind {
    /// Config was created (or re-created after delete).
    Created,
    /// Config was updated.
    Updated,
    /// Config was deleted.
    Deleted,
}

/// Durable push config store with node-local scheduling watermarks.
#[derive(Clone)]
pub struct A2APushConfigStore {
    store: A2APushConfigStateStore,
    credential_policy: A2APushCredentialPolicy,
    binding_resolver: Option<Arc<dyn A2ACredentialBindingResolver>>,
    /// Highest event sequence per (tenant, task id) whose push effects were
    /// durably scheduled (or confirmed unnecessary). Node-local memory: on
    /// loss the retained event log is re-offered and the durable
    /// deduplication keys drop anything scheduled twice.
    scheduled: Arc<Mutex<BTreeMap<(String, String), u64>>>,
}

impl std::fmt::Debug for A2APushConfigStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("A2APushConfigStore")
            .field("credential_policy", &self.credential_policy)
            .field("binding_resolver", &self.binding_resolver.is_some())
            .finish_non_exhaustive()
    }
}

impl A2APushConfigStore {
    /// Creates a push config store over any durable state backend.
    #[must_use]
    pub fn new(store: impl DurableStateStore<A2APushConfigState>) -> Self {
        Self {
            store: SharedDurableStateStore::new(store),
            credential_policy: A2APushCredentialPolicy::default(),
            binding_resolver: None,
            scheduled: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Overrides the raw-credential policy. See
    /// [`A2APushCredentialPolicy`]; the default rejects raw credentials.
    #[must_use]
    pub fn with_credential_policy(mut self, policy: A2APushCredentialPolicy) -> Self {
        self.credential_policy = policy;
        self
    }

    /// Installs the application's credential binding resolver.
    #[must_use]
    pub fn with_credential_binding_resolver(
        mut self,
        resolver: Arc<dyn A2ACredentialBindingResolver>,
    ) -> Self {
        self.binding_resolver = Some(resolver);
        self
    }

    pub(crate) fn scheduled_watermark(&self, tenant: &str, task_id: &str) -> u64 {
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
    pub(crate) fn record_scheduled_watermark(&self, tenant: &str, task_id: &str, sequence: u64) {
        self.scheduled
            .lock()
            .expect("push watermark mutex")
            .insert((tenant.to_string(), task_id.to_string()), sequence);
    }

    /// Saves (creates or updates) a push config, redacting credentials.
    ///
    /// Reads and writes only the single `(tenant, task_id)` record, so cost is
    /// bounded by the configs on that task rather than the whole store.
    /// Concurrent writers to the same task record are re-driven a bounded
    /// number of times on optimistic-concurrency conflicts.
    pub async fn save(
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
        let binding_ref = self.resolve_binding(tenant, &config)?;

        if config.id.as_deref().is_none_or(str::is_empty) {
            config.id = Some(generated_config_id());
        }
        let config_id = config.id.clone().expect("config id set");
        let task_id = config.task_id.clone();
        let persistence_id = push_config_persistence_id(tenant, &task_id);
        let auth = auth_metadata(&config, binding_ref);
        let redacted = redacted_config(config);

        for _ in 0..MAX_PUSH_CONFIG_WRITE_ATTEMPTS {
            let now = AgentTimestampMillis::new(current_timestamp_millis());
            let (expected, mut state) = match self.store.load(&persistence_id).await? {
                Some(record) => (record.revision, record.state),
                None => (
                    Revision::INITIAL,
                    A2APushConfigState {
                        tenant: tenant.to_string(),
                        task_id: task_id.clone(),
                        configs: Vec::new(),
                    },
                ),
            };

            if let Some(entry) = state
                .configs
                .iter_mut()
                .find(|entry| entry.config_id == config_id)
            {
                // Request-level configs arrive on every send; an identical
                // re-save is a no-op read rather than a durable rewrite with
                // audit churn.
                if !entry.deleted && entry.config == redacted && entry.auth == auth {
                    return Ok(entry.config.clone());
                }
                let kind = if entry.deleted {
                    A2APushConfigAuditKind::Created
                } else {
                    A2APushConfigAuditKind::Updated
                };
                entry.config = redacted.clone();
                entry.auth = auth.clone();
                entry.deleted = false;
                entry.updated_at = now;
                entry.audit.push(A2APushConfigAuditRecord {
                    kind,
                    occurred_at: now,
                    redaction: auth.redaction,
                });
                cap_audit(&mut entry.audit);
            } else {
                state.configs.push(A2APushConfigEntry {
                    config_id: config_id.clone(),
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
                });
                cap_configs(&mut state.configs);
            }

            match self
                .store
                .compare_and_set(&persistence_id, expected, state)
                .await
            {
                Ok(_) => return Ok(redacted),
                Err(DurableError::RevisionConflict { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(A2APushConfigError::Persistence(DurableError::store(
            self.store.backend_name(),
            "push config save contention exceeded retry bound",
        )))
    }

    /// Applies the DN-4 credential-binding policy: reject, never hold.
    fn resolve_binding(
        &self,
        tenant: &str,
        config: &TaskPushNotificationConfig,
    ) -> A2APushConfigResult<Option<A2ACredentialBindingRef>> {
        if let Some(resolver) = &self.binding_resolver {
            return resolver.resolve(tenant, config);
        }
        let has_secret_material = config.token.is_some()
            || config
                .authentication
                .as_ref()
                .and_then(|authentication| authentication.credentials.as_ref())
                .is_some();
        if has_secret_material
            && self.credential_policy == A2APushCredentialPolicy::RejectRawCredentials
        {
            return Err(A2APushConfigError::InvalidConfig {
                field: "authentication",
                reason: "raw push credentials are rejected; configure a credential \
                         binding resolver or remove inline credentials",
            });
        }
        Ok(None)
    }

    /// Reads one active push config.
    pub async fn get(
        &self,
        tenant: &str,
        task_id: &str,
        config_id: &str,
    ) -> A2APushConfigResult<TaskPushNotificationConfig> {
        validate_tenant(tenant)?;
        self.load_state(tenant, task_id)
            .await?
            .and_then(|state| {
                state
                    .configs
                    .into_iter()
                    .find(|entry| entry.config_id == config_id && !entry.deleted)
            })
            .map(|entry| entry.config)
            .ok_or_else(|| A2APushConfigError::ConfigNotFound {
                task_id: task_id.to_string(),
                config_id: config_id.to_string(),
            })
    }

    /// Lists active push configs for one task with deterministic pagination.
    pub async fn list(
        &self,
        tenant: &str,
        request: &ListTaskPushNotificationConfigsRequest,
    ) -> A2APushConfigResult<ListTaskPushNotificationConfigsResponse> {
        validate_tenant(tenant)?;
        let offset = page_offset(request.page_token.as_deref())?;
        let page_size = page_size(request.page_size);
        let mut entries = self.active_entries(tenant, &request.task_id).await?;
        entries.sort_by(|left, right| left.config_id.cmp(&right.config_id));

        let configs = entries
            .iter()
            .skip(offset)
            .take(page_size)
            .map(|entry| entry.config.clone())
            .collect::<Vec<_>>();
        let next_offset = offset.saturating_add(configs.len());
        let next_page_token = (next_offset < entries.len()).then(|| next_offset.to_string());
        Ok(ListTaskPushNotificationConfigsResponse {
            configs,
            next_page_token,
        })
    }

    /// Soft-deletes one push config; deleting a missing config is a no-op.
    pub async fn delete(
        &self,
        tenant: &str,
        task_id: &str,
        config_id: &str,
    ) -> A2APushConfigResult<()> {
        validate_tenant(tenant)?;
        let persistence_id = push_config_persistence_id(tenant, task_id);
        for _ in 0..MAX_PUSH_CONFIG_WRITE_ATTEMPTS {
            let Some(record) = self.store.load(&persistence_id).await? else {
                return Ok(());
            };
            let expected = record.revision;
            let mut state = record.state;
            let Some(entry) = state
                .configs
                .iter_mut()
                .find(|entry| entry.config_id == config_id && !entry.deleted)
            else {
                return Ok(());
            };
            let now = AgentTimestampMillis::new(current_timestamp_millis());
            entry.deleted = true;
            entry.updated_at = now;
            entry.audit.push(A2APushConfigAuditRecord {
                kind: A2APushConfigAuditKind::Deleted,
                occurred_at: now,
                redaction: entry.auth.redaction,
            });
            cap_audit(&mut entry.audit);
            match self
                .store
                .compare_and_set(&persistence_id, expected, state)
                .await
            {
                Ok(_) => return Ok(()),
                Err(DurableError::RevisionConflict { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(A2APushConfigError::Persistence(DurableError::store(
            self.store.backend_name(),
            "push config delete contention exceeded retry bound",
        )))
    }

    /// Lists active configs for scheduling.
    pub async fn active_configs(
        &self,
        tenant: &str,
        task_id: &str,
    ) -> A2APushConfigResult<Vec<TaskPushNotificationConfig>> {
        Ok(self
            .active_entries(tenant, task_id)
            .await?
            .into_iter()
            .map(|entry| entry.config)
            .collect())
    }

    /// Loads the single `(tenant, task_id)` push-config record, if present.
    async fn load_state(
        &self,
        tenant: &str,
        task_id: &str,
    ) -> A2APushConfigResult<Option<A2APushConfigState>> {
        Ok(self
            .store
            .load(&push_config_persistence_id(tenant, task_id))
            .await?
            .map(|record| record.state))
    }

    /// Returns the active (non-deleted) config entries for one task.
    async fn active_entries(
        &self,
        tenant: &str,
        task_id: &str,
    ) -> A2APushConfigResult<Vec<A2APushConfigEntry>> {
        let Some(state) = self.load_state(tenant, task_id).await? else {
            return Ok(Vec::new());
        };
        Ok(state
            .configs
            .into_iter()
            .filter(|entry| !entry.deleted)
            .collect())
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
pub async fn schedule_push_effects_for_events<WorkflowStoreT>(
    workflow_store: &WorkflowStoreT,
    push_configs: &A2APushConfigStore,
    task_store: &dyn crate::projection::A2ATaskProjectionStore,
    tenant: &str,
    task_id: &str,
    newly_emitted: &[A2ATaskEvent],
) -> A2APushConfigResult<usize>
where
    WorkflowStoreT: DurableStateStore<WorkflowState>,
{
    let watermark = push_configs.scheduled_watermark(tenant, task_id);
    let cursor = encode_replay_cursor(task_id, watermark);
    let pending = match task_store
        .replay_events(tenant, task_id, Some(&cursor))
        .await
    {
        Ok(events) => events,
        // The log cannot resume from the watermark (compaction, or an owner
        // epoch change reset the sequences). Events older than the retained
        // window are past retention policy; schedule what the caller just
        // emitted and re-anchor the watermark to it.
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

/// Effect target type used for A2A push notification webhooks.
pub const PUSH_EFFECT_TARGET_TYPE: &str = "notification";
/// Effect target name used for A2A push notification webhooks.
pub const PUSH_EFFECT_TARGET_NAME: &str = "a2a-push-webhook";
/// Effect target attribute key carrying the owning tenant.
pub const PUSH_ATTR_TENANT: &str = "a2a_tenant";
/// Effect target attribute key carrying the task id.
pub const PUSH_ATTR_TASK_ID: &str = "a2a_task_id";
/// Effect target attribute key carrying the push config id.
pub const PUSH_ATTR_CONFIG_ID: &str = "a2a_config_id";
/// Effect target attribute key carrying the task-event sequence.
pub const PUSH_ATTR_SEQUENCE: &str = "a2a_task_event_sequence";
/// Effect target attribute key carrying the task-event kind label.
pub const PUSH_ATTR_EVENT_KIND: &str = "task_event_kind";
/// Effect target attribute key carrying the public task-state label.
pub const PUSH_ATTR_TASK_STATE: &str = "task_state";
/// Effect target attribute key carrying the redaction label.
pub const PUSH_ATTR_REDACTION: &str = "redaction";

/// Builds the durable push notification effect for one event and config.
///
/// The effect carries only the callback URL plus bounded, non-secret labels;
/// the stable idempotency key `a2a-push:<task>:<sequence>:<config>` makes
/// re-offered events deduplicate in the durable outbox. No credential material
/// is ever placed on the effect (DN-4); the dispatcher resolves auth from the
/// application's binding at send time.
pub(crate) fn push_effect(
    event: &A2ATaskEvent,
    config: &TaskPushNotificationConfig,
) -> A2APushConfigResult<AgentEffect> {
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
    attributes.insert(PUSH_ATTR_TENANT.to_string(), event.tenant.clone());
    attributes.insert(PUSH_ATTR_TASK_ID.to_string(), event.task_id.clone());
    attributes.insert(PUSH_ATTR_CONFIG_ID.to_string(), config_id.to_string());
    attributes.insert(PUSH_ATTR_SEQUENCE.to_string(), event.sequence.to_string());
    attributes.insert(
        PUSH_ATTR_EVENT_KIND.to_string(),
        event.kind().as_label().to_string(),
    );
    attributes.insert(
        PUSH_ATTR_TASK_STATE.to_string(),
        task_state_label(&event.projected_state).to_string(),
    );
    attributes.insert(
        PUSH_ATTR_REDACTION.to_string(),
        event.redaction.as_label().to_string(),
    );

    let target = AgentEffectTarget {
        target_type: PUSH_EFFECT_TARGET_TYPE.to_string(),
        name: PUSH_EFFECT_TARGET_NAME.to_string(),
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

/// Keeps the per-task record bounded at `MAX_PUSH_CONFIGS_PER_TASK`, evicting
/// the oldest soft-deleted entry first and otherwise the oldest by creation
/// time, so live configs survive longer than tombstones.
fn cap_configs(configs: &mut Vec<A2APushConfigEntry>) {
    while configs.len() > MAX_PUSH_CONFIGS_PER_TASK {
        let victim = configs
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.deleted)
            .min_by_key(|(_, entry)| entry.updated_at.as_millis())
            .map(|(index, _)| index)
            .or_else(|| {
                configs
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, entry)| entry.created_at.as_millis())
                    .map(|(index, _)| index)
            });
        match victim {
            Some(index) => {
                configs.remove(index);
            }
            None => break,
        }
    }
}

fn redacted_config(mut config: TaskPushNotificationConfig) -> TaskPushNotificationConfig {
    config.token = None;
    if let Some(authentication) = config.authentication.as_mut() {
        authentication.credentials = None;
    }
    config
}

fn auth_metadata(
    config: &TaskPushNotificationConfig,
    credential_binding_ref: Option<A2ACredentialBindingRef>,
) -> A2APushConfigAuthMetadata {
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
        credential_binding_ref,
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

fn push_config_persistence_id(tenant: &str, task_id: &str) -> PersistenceId {
    PersistenceId::new(format!(
        "{PUSH_CONFIG_PERSISTENCE_PREFIX}:{}:{}",
        hex_encode(tenant),
        hex_encode(task_id)
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
    use rakka_persistence::InMemoryDurableStateStore;

    fn permissive_store() -> A2APushConfigStore {
        A2APushConfigStore::new(InMemoryDurableStateStore::<A2APushConfigState>::new())
            .with_credential_policy(A2APushCredentialPolicy::RedactAndRecordPresence)
    }

    fn rejecting_store() -> A2APushConfigStore {
        A2APushConfigStore::new(InMemoryDurableStateStore::<A2APushConfigState>::new())
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

    fn credential_free_config(task_id: &str, id: &str) -> TaskPushNotificationConfig {
        TaskPushNotificationConfig {
            token: None,
            authentication: None,
            ..config(task_id, id)
        }
    }

    async fn load_entry(
        durable: &InMemoryDurableStateStore<A2APushConfigState>,
        tenant: &str,
        task_id: &str,
        config_id: &str,
    ) -> A2APushConfigEntry {
        durable
            .load(&push_config_persistence_id(tenant, task_id))
            .await
            .expect("load")
            .expect("state")
            .state
            .configs
            .into_iter()
            .find(|entry| entry.config_id == config_id)
            .expect("entry present")
    }

    #[tokio::test]
    async fn raw_credentials_are_rejected_by_default() {
        let store = rejecting_store();
        let error = store
            .save("tenant-a", config("task-1", "cfg-1"))
            .await
            .expect_err("raw credentials must be rejected");
        assert_eq!(error.code(), "invalid-push-config");

        // Credential-free configs remain accepted under the default policy.
        store
            .save("tenant-a", credential_free_config("task-1", "cfg-2"))
            .await
            .expect("credential-free config");
    }

    #[tokio::test]
    async fn binding_resolver_persists_reference_never_secret_material() {
        struct FixedResolver;
        impl A2ACredentialBindingResolver for FixedResolver {
            fn resolve(
                &self,
                tenant: &str,
                _config: &TaskPushNotificationConfig,
            ) -> A2APushConfigResult<Option<A2ACredentialBindingRef>> {
                Ok(Some(A2ACredentialBindingRef::new(format!(
                    "binding:{tenant}:webhook"
                ))))
            }
        }

        let durable = InMemoryDurableStateStore::<A2APushConfigState>::new();
        let store = A2APushConfigStore::new(durable.clone())
            .with_credential_binding_resolver(Arc::new(FixedResolver));
        let saved = store
            .save("tenant-a", config("task-1", "cfg-1"))
            .await
            .expect("save with binding resolver");
        assert!(saved.token.is_none());

        let entry = load_entry(&durable, "tenant-a", "task-1", "cfg-1").await;
        assert_eq!(
            entry.auth.credential_binding_ref,
            Some(A2ACredentialBindingRef::new("binding:tenant-a:webhook"))
        );
        assert!(entry.config.token.is_none());
        let serialized = serde_json::to_string(&entry).expect("entry json");
        assert!(
            !serialized.contains("secret"),
            "persisted state must not contain raw credentials: {serialized}"
        );
    }

    #[tokio::test]
    async fn save_get_list_and_delete_redacts_auth_material() {
        let durable = InMemoryDurableStateStore::<A2APushConfigState>::new();
        let store = A2APushConfigStore::new(durable.clone())
            .with_credential_policy(A2APushCredentialPolicy::RedactAndRecordPresence);
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
        let entry = load_entry(&durable, "tenant-a", "task-1", "cfg-1").await;
        assert!(entry.auth.token_present);
        assert!(entry.auth.credentials_present);
        assert!(entry.config.token.is_none());

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
        let store = permissive_store();
        let mut config = config("task-1", "cfg-1");
        config.url = "file:///tmp/hook".to_string();
        let error = store.save("tenant-a", config).await.expect_err("invalid");
        assert_eq!(error.code(), "invalid-push-config");
    }

    /// Durable store that refuses `persistence_ids` (inheriting the trait
    /// default), mirroring backends without a store-wide id scan. Push config
    /// storage must work entirely from `(tenant, task)` loads over it.
    #[derive(Clone)]
    struct NoScanStore(InMemoryDurableStateStore<A2APushConfigState>);

    impl DurableStateStore<A2APushConfigState> for NoScanStore {
        fn backend_name(&self) -> &'static str {
            "no-scan"
        }

        fn load<'a>(
            &'a self,
            persistence_id: &'a PersistenceId,
        ) -> rakka_persistence::StoreFuture<
            'a,
            Option<rakka_persistence::StateRecord<A2APushConfigState>>,
        > {
            self.0.load(persistence_id)
        }

        fn compare_and_set<'a>(
            &'a self,
            persistence_id: &'a PersistenceId,
            expected_revision: Revision,
            state: A2APushConfigState,
        ) -> rakka_persistence::StoreFuture<'a, rakka_persistence::StateRecord<A2APushConfigState>>
        {
            self.0
                .compare_and_set(persistence_id, expected_revision, state)
        }

        fn delete<'a>(
            &'a self,
            persistence_id: &'a PersistenceId,
            expected_revision: Revision,
        ) -> rakka_persistence::StoreFuture<'a, Revision> {
            self.0.delete(persistence_id, expected_revision)
        }
        // persistence_ids is intentionally not overridden: the trait default
        // errors, so this test fails loudly if any path scans the store.
    }

    #[tokio::test]
    async fn store_operations_do_not_scan_persistence_ids() {
        let store = A2APushConfigStore::new(NoScanStore(InMemoryDurableStateStore::new()))
            .with_credential_policy(A2APushCredentialPolicy::RedactAndRecordPresence);
        store
            .save("tenant-a", config("task-1", "cfg-1"))
            .await
            .expect("save cfg-1");
        store
            .save("tenant-a", config("task-1", "cfg-2"))
            .await
            .expect("save cfg-2");

        store
            .get("tenant-a", "task-1", "cfg-1")
            .await
            .expect("get without scan");
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
            .expect("list without scan");
        assert_eq!(listed.configs.len(), 2);
        assert_eq!(
            store
                .active_configs("tenant-a", "task-1")
                .await
                .expect("active without scan")
                .len(),
            2
        );

        store
            .delete("tenant-a", "task-1", "cfg-1")
            .await
            .expect("delete without scan");
        assert_eq!(
            store
                .active_configs("tenant-a", "task-1")
                .await
                .expect("active after delete")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn per_task_configs_are_bounded() {
        let store = permissive_store();
        for index in 0..(MAX_PUSH_CONFIGS_PER_TASK + 10) {
            store
                .save("tenant-a", config("task-1", &format!("cfg-{index}")))
                .await
                .expect("save");
        }
        assert!(
            store
                .active_configs("tenant-a", "task-1")
                .await
                .expect("active")
                .len()
                <= MAX_PUSH_CONFIGS_PER_TASK,
            "per-task config count must stay bounded"
        );
    }

    #[tokio::test]
    async fn identical_resave_skips_audit_and_audit_stays_bounded() {
        let durable = InMemoryDurableStateStore::<A2APushConfigState>::new();
        let store = A2APushConfigStore::new(durable.clone())
            .with_credential_policy(A2APushCredentialPolicy::RedactAndRecordPresence);
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
        let entry = load_entry(&durable, "tenant-a", "task-1", "cfg-1").await;
        assert_eq!(
            entry.audit.len(),
            1,
            "identical re-save must not append audit records"
        );

        // Genuine updates keep the audit trail bounded.
        for index in 0..(MAX_PUSH_CONFIG_AUDIT_RECORDS + 8) {
            let mut updated = config("task-1", "cfg-1");
            updated.url = format!("https://example.com/hook-{index}");
            store.save("tenant-a", updated).await.expect("update");
        }
        let entry = load_entry(&durable, "tenant-a", "task-1", "cfg-1").await;
        assert!(
            entry.audit.len() <= MAX_PUSH_CONFIG_AUDIT_RECORDS,
            "audit trail must stay bounded, got {}",
            entry.audit.len()
        );
    }

    #[tokio::test]
    async fn watermark_catchup_heals_missed_schedules_and_deduplicates() {
        use crate::projection::{A2ATaskProjectionStore, InMemoryA2ATaskProjectionStore};
        use crate::task::{A2ATaskEventPayload, A2ATaskProjection};
        use rakka_agent_workflow::substrate::WorkflowState;

        let store = permissive_store();
        store
            .save("tenant-a", config("task-1", "cfg-1"))
            .await
            .expect("save config");
        let workflow_store = InMemoryDurableStateStore::<WorkflowState>::new();
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
            .await
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
            .await
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
            .await
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
        use rakka_agent_workflow::substrate::WorkflowId;

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
