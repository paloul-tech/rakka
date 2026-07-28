//! Shared wake scanner: recovers durable occurrences and injects deduplicated
//! admission commands.
//!
//! The scanner is a caller-driven bounded pass, exactly like the choreography
//! courier and the dispatch pump: it re-drives from durable state under
//! operation ids the occurrences themselves derive, so calling it after a
//! crash, on a timer, from two pods at once, or never for a week are all the
//! same operation. A pass reads the due entries of one
//! [`AgentWakeTimerStore`], delivers each as the
//! [`AgentTaskEntityCommand::AdmitWake`] its binding derives, and records the
//! terminal status the controller's disposition earned — fired, or fenced for
//! an obsolete schedule revision.
//!
//! Scanner and pod uptime never create an occurrence
//! ([specification 15](../../../docs/plans/rakka-agent/spec.md)): a pass over
//! an empty store does nothing, and a crash between delivery and mark-fired
//! redelivers into the controller's deduplication, which answers a duplicate
//! rather than admitting twice.
//!
//! A pass preserves per-task due order: after a failed or refused delivery,
//! every later-due entry of the same task is held back for the next pass
//! rather than delivered around the failure. The controller's
//! scheduled-occurrence watermark deduplicates on the invariant that a task's
//! scheduled occurrences arrive in due order; delivering past a failure would
//! let a later occurrence advance the watermark over an earlier one that was
//! never applied, and its redelivery would then be swallowed as a false
//! duplicate.
//!
//! Deployment topology: any number of scanners may run, and every delivery is
//! safe to duplicate. The provided [`ShardedWakeDelivery`] delivers to task
//! entities this node owns and reports a stable `wake-remote-owner` failure
//! for the rest, leaving those entries pending for the owning node's own
//! scanner — delivery follows shard ownership, so no wake ever needs a
//! cross-node command surface.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rakka_agent_workflow::substrate::{SystemWorkflowClock, WorkflowClock};
use rakka_agent_workflow::AgentTimestampMillis;
use rakka_core::{MetricsRecorder, NoopMetricsRecorder};
use rakka_persistence::DurableStateStore;
use rakka_sharding::ClusterSharding;

use crate::choreography::AgentExchangeDeliveryError;
use crate::identity::{AgentIdentityError, AgentTaskId, AgentTaskScope, AgentWakeId, TenantId};
use crate::observability::record_agent_domain_counter;
use crate::task::{
    agent_task_entity_ref, AgentTaskEntityCommand, AgentTaskEntityMessage, AgentTaskEntityReply,
    AgentTaskEntityTypeKey,
};
use crate::wake::{
    AgentWakeBinding, AgentWakeDisposition, AgentWakeError, AgentWakeOutcome, AgentWakeResult,
};
use crate::wake_timers::{
    AgentWakeTimerEntry, AgentWakeTimerError, AgentWakeTimerStatus, AgentWakeTimerStore,
    AgentWakeTimerStoreState,
};

/// Counter for wake delivery attempts, labelled by bounded outcome and
/// trigger class.
pub const METRIC_AGENT_WAKES: &str = "rakka.agent.wakes";

/// Result type for wake scanning.
pub type AgentWakeScanResult<T> = Result<T, AgentWakeScanError>;

/// Builds the admission command a wake binding derives.
///
/// Every trigger path — the shared scanner, an external event's ingress, an
/// authenticated A2A command, a callback — must construct the command through
/// this one function, so the operation id is always the binding's own derived
/// admission operation id and duplicate delivery deduplicates by
/// construction.
pub fn wake_admission_command(
    binding: AgentWakeBinding,
) -> AgentWakeResult<AgentTaskEntityCommand> {
    let operation_id = binding.admission_operation_id()?;
    Ok(AgentTaskEntityCommand::AdmitWake {
        operation_id,
        binding: Box::new(binding),
    })
}

/// Boxed future returned by a wake delivery.
pub type AgentWakeDeliveryFuture<'a> = Pin<
    Box<dyn Future<Output = Result<AgentTaskEntityReply, AgentExchangeDeliveryError>> + Send + 'a>,
>;

/// Delivers one admission command to the root control task that owns it.
///
/// Delivery is at-most-once, like every other message in Rakka. A failure is
/// never evidence the controller did not apply the command; the only safe
/// response is to leave the entry pending and redeliver the same derived
/// operation id on a later pass.
pub trait AgentWakeDelivery: Send + Sync {
    /// Attempts one delivery.
    fn deliver<'a>(
        &'a self,
        scope: &'a AgentTaskScope,
        command: AgentTaskEntityCommand,
    ) -> AgentWakeDeliveryFuture<'a>;
}

/// Wake scanner settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentWakeScannerSettings {
    max_batch_size: usize,
}

impl AgentWakeScannerSettings {
    /// Creates scanner settings.
    #[must_use]
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            max_batch_size: max_batch_size.max(1),
        }
    }

    /// Most occurrences delivered in one pass.
    #[must_use]
    pub const fn max_batch_size(self) -> usize {
        self.max_batch_size
    }
}

impl Default for AgentWakeScannerSettings {
    fn default() -> Self {
        Self::new(32)
    }
}

/// Scanner that recovers due wake occurrences and injects their admission
/// commands.
pub struct AgentWakeScanner<Store, Delivery, Clock = SystemWorkflowClock>
where
    Store: DurableStateStore<AgentWakeTimerStoreState>,
    Delivery: AgentWakeDelivery,
    Clock: WorkflowClock,
{
    timers: AgentWakeTimerStore<Store>,
    delivery: Delivery,
    clock: Clock,
    settings: AgentWakeScannerSettings,
    metrics: Arc<dyn MetricsRecorder>,
}

impl<Store, Delivery> AgentWakeScanner<Store, Delivery, SystemWorkflowClock>
where
    Store: DurableStateStore<AgentWakeTimerStoreState>,
    Delivery: AgentWakeDelivery,
{
    /// Creates a scanner with the system clock and no-op metrics.
    #[must_use]
    pub fn new(timers: AgentWakeTimerStore<Store>, delivery: Delivery) -> Self {
        Self::with_clock_and_metrics(
            timers,
            delivery,
            SystemWorkflowClock,
            AgentWakeScannerSettings::default(),
            Arc::new(NoopMetricsRecorder),
        )
    }
}

impl<Store, Delivery, Clock> AgentWakeScanner<Store, Delivery, Clock>
where
    Store: DurableStateStore<AgentWakeTimerStoreState>,
    Delivery: AgentWakeDelivery,
    Clock: WorkflowClock,
{
    /// Creates a scanner with an explicit clock, settings, and metrics.
    #[must_use]
    pub fn with_clock_and_metrics(
        timers: AgentWakeTimerStore<Store>,
        delivery: Delivery,
        clock: Clock,
        settings: AgentWakeScannerSettings,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self {
            timers,
            delivery,
            clock,
            settings,
            metrics,
        }
    }

    /// Durable wake-timer store.
    #[must_use]
    pub const fn timers(&self) -> &AgentWakeTimerStore<Store> {
        &self.timers
    }

    /// Mutably accesses the durable wake-timer store.
    #[must_use]
    pub fn timers_mut(&mut self) -> &mut AgentWakeTimerStore<Store> {
        &mut self.timers
    }

    /// Recovers and delivers due occurrences, bounded by the configured batch
    /// size.
    ///
    /// Deliveries preserve per-task due order: after a failed or refused
    /// delivery, every later-due entry of the same task is
    /// [held back](AgentWakeScanOutcome::HeldBack) for the next pass instead
    /// of delivered around the failure, so the controller's scheduled-due-time
    /// watermark can never be advanced over an occurrence that was never
    /// applied.
    pub async fn scan_due(&mut self) -> AgentWakeScanResult<AgentWakeScan> {
        let now = AgentTimestampMillis::new(self.clock.now().as_millis());
        let due_count = self.timers.due_entry_count(now).await?;
        let due = self
            .timers
            .due_entries(now, self.settings.max_batch_size)
            .await?;
        let mut outcomes = Vec::with_capacity(due.len());
        let mut blocked: BTreeMap<(TenantId, AgentTaskId), AgentWakeId> = BTreeMap::new();
        for entry in due {
            let task_key = (entry.binding().tenant().clone(), entry.task().clone());
            if let Some(blocked_by) = blocked.get(&task_key) {
                self.record_wake_metric("held-back", entry.binding().trigger().as_label());
                outcomes.push(AgentWakeScanOutcome::HeldBack {
                    wake: entry.wake_id().clone(),
                    blocked_by: blocked_by.clone(),
                });
                continue;
            }
            let outcome = self.deliver_entry(&entry, now).await?;
            if matches!(
                outcome,
                AgentWakeScanOutcome::Failed { .. } | AgentWakeScanOutcome::Rejected { .. }
            ) {
                blocked.insert(task_key, entry.wake_id().clone());
            }
            outcomes.push(outcome);
        }
        let delivered = outcomes.len();
        Ok(AgentWakeScan {
            scanned_at: now,
            due_count,
            max_batch_size: self.settings.max_batch_size,
            backpressure_limited: due_count > delivered,
            outcomes,
        })
    }

    async fn deliver_entry(
        &mut self,
        entry: &AgentWakeTimerEntry,
        now: AgentTimestampMillis,
    ) -> AgentWakeScanResult<AgentWakeScanOutcome> {
        let scope = AgentTaskScope::new(entry.binding().tenant().clone(), entry.task().clone())?;
        let command = wake_admission_command(entry.binding().clone())?;
        let trigger = entry.binding().trigger().as_label();
        let reply = match self.delivery.deliver(&scope, command).await {
            Ok(reply) => reply,
            Err(error) => {
                // The entry stays pending: a failure is not evidence the
                // controller did not apply, and the next pass redelivers the
                // same derived operation id.
                self.record_wake_metric("delivery-failed", trigger);
                return Ok(AgentWakeScanOutcome::Failed {
                    wake: entry.wake_id().clone(),
                    code: error.code().to_string(),
                });
            }
        };
        let (outcome, redelivery) = match reply {
            AgentTaskEntityReply::Applied { outcome } => (outcome, false),
            AgentTaskEntityReply::Duplicate { outcome } => (outcome, true),
            AgentTaskEntityReply::Rejected { code, .. } => {
                // A rejection is a refusal, not a disposition: the entry stays
                // pending so a transient refusal re-drives, and an operator
                // can cancel an entry a permanent refusal leaves behind.
                self.record_wake_metric("rejected", trigger);
                return Ok(AgentWakeScanOutcome::Rejected {
                    wake: entry.wake_id().clone(),
                    code,
                });
            }
            AgentTaskEntityReply::Snapshot(_) | AgentTaskEntityReply::Progressed { .. } => {
                self.record_wake_metric("rejected", trigger);
                return Ok(AgentWakeScanOutcome::Rejected {
                    wake: entry.wake_id().clone(),
                    code: "wake-unexpected-reply".to_string(),
                });
            }
        };
        let disposition = match outcome.wake {
            Some(AgentWakeOutcome::Disposition(disposition)) => disposition,
            // An admission operation id can only ever record a disposition;
            // answer conservatively if the outcome carries none.
            _ => AgentWakeDisposition::Duplicate {
                wake: entry.wake_id().clone(),
            },
        };
        let marked = if matches!(disposition, AgentWakeDisposition::Fenced { .. }) {
            // A fenced entry is terminal in the store too, so an obsolete
            // occurrence is never rescanned forever.
            self.timers.mark_fenced(entry.wake_id(), now).await?;
            AgentWakeTimerStatus::Fenced
        } else {
            self.timers.mark_fired(entry.wake_id(), now).await?;
            AgentWakeTimerStatus::Fired
        };
        self.record_wake_metric(disposition.as_label(), trigger);
        Ok(AgentWakeScanOutcome::Dispositioned {
            wake: entry.wake_id().clone(),
            disposition,
            redelivery,
            marked,
        })
    }

    fn record_wake_metric(&self, outcome: &'static str, trigger: &'static str) {
        record_agent_domain_counter(
            self.metrics.as_ref(),
            METRIC_AGENT_WAKES,
            1,
            &[("outcome", outcome), ("trigger", trigger)],
        )
        .ok();
    }
}

/// What one delivery of a scan pass concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentWakeScanOutcome {
    /// The controller dispositioned the occurrence, and the entry was marked
    /// with the terminal status the disposition earned.
    Dispositioned {
        /// The delivered wake.
        wake: AgentWakeId,
        /// How the controller dispositioned it.
        disposition: AgentWakeDisposition,
        /// Whether the controller had already dispositioned it — a crash
        /// between an earlier delivery and its mark, or a concurrent scanner.
        redelivery: bool,
        /// The terminal status recorded in the store.
        marked: AgentWakeTimerStatus,
    },
    /// The controller refused the command; the entry stays pending.
    Rejected {
        /// The refused wake.
        wake: AgentWakeId,
        /// The stable refusal code.
        code: String,
    },
    /// The delivery attempt itself failed; the entry stays pending.
    Failed {
        /// The undelivered wake.
        wake: AgentWakeId,
        /// The stable delivery-failure code.
        code: String,
    },
    /// The entry was held back with no delivery attempt: an earlier-due
    /// occurrence of the same task failed or was refused in this pass, and
    /// the entry stays pending so the task's scheduled occurrences keep
    /// arriving in due order — the invariant the controller's due-time
    /// watermark deduplicates on.
    HeldBack {
        /// The held wake.
        wake: AgentWakeId,
        /// The earlier-due wake whose failure or refusal blocked the task for
        /// the rest of the pass.
        blocked_by: AgentWakeId,
    },
}

/// Result of one bounded wake scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentWakeScan {
    /// When the pass ran, by the scanner's clock.
    pub scanned_at: AgentTimestampMillis,
    /// How many entries were pending and due when the pass started.
    pub due_count: usize,
    /// The configured batch bound.
    pub max_batch_size: usize,
    /// Whether more entries were due than one pass may deliver.
    pub backpressure_limited: bool,
    /// What each delivery concluded, in due order.
    pub outcomes: Vec<AgentWakeScanOutcome>,
}

/// Production wake delivery over sharded task entities.
///
/// It asks the locally owned entity and reports a stable `wake-remote-owner`
/// failure for a task another node owns, leaving that entry pending for the
/// owning node's scanner: delivery follows shard ownership, so a wake needs no
/// cross-node command surface and two nodes' scanners never race beyond the
/// controller's deduplication.
pub struct ShardedWakeDelivery {
    sharding: ClusterSharding,
    key: AgentTaskEntityTypeKey,
    ask_timeout: Duration,
}

impl ShardedWakeDelivery {
    /// Creates a delivery over one sharded task-entity registration.
    #[must_use]
    pub const fn new(
        sharding: ClusterSharding,
        key: AgentTaskEntityTypeKey,
        ask_timeout: Duration,
    ) -> Self {
        Self {
            sharding,
            key,
            ask_timeout,
        }
    }
}

impl AgentWakeDelivery for ShardedWakeDelivery {
    fn deliver<'a>(
        &'a self,
        scope: &'a AgentTaskScope,
        command: AgentTaskEntityCommand,
    ) -> AgentWakeDeliveryFuture<'a> {
        Box::pin(async move {
            let entity =
                agent_task_entity_ref(&self.sharding, &self.key, scope).map_err(|error| {
                    AgentExchangeDeliveryError::new("wake-no-route", error.to_string())
                })?;
            let (owner, _shard) =
                entity
                    .region()
                    .resolve(entity.entity_ref())
                    .map_err(|error| {
                        AgentExchangeDeliveryError::new("wake-no-route", error.to_string())
                    })?;
            let is_local = entity
                .region()
                .local_node_id()
                .is_some_and(|local| local == &owner);
            if !is_local {
                return Err(AgentExchangeDeliveryError::new(
                    "wake-remote-owner",
                    format!("task {scope} is owned by {owner:?}; its owner's scanner delivers it"),
                ));
            }
            entity
                .ask(
                    move |reply_to| AgentTaskEntityMessage::Command {
                        command: Box::new(command),
                        reply_to,
                    },
                    self.ask_timeout,
                )
                .await
                .map_err(|error| {
                    AgentExchangeDeliveryError::new("wake-ask-failed", error.to_string())
                })
        })
    }
}

/// Wake-scan failures.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AgentWakeScanError {
    /// The durable wake-timer store refused the pass.
    Timers(AgentWakeTimerError),
    /// The wake contract refused a command construction.
    Wake(AgentWakeError),
    /// An entry's identities could not key a durable scope.
    Identity(AgentIdentityError),
}

impl AgentWakeScanError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Timers(error) => error.code(),
            Self::Wake(error) => error.code(),
            Self::Identity(error) => error.code(),
        }
    }
}

impl std::fmt::Display for AgentWakeScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timers(error) => std::fmt::Display::fmt(error, f),
            Self::Wake(error) => std::fmt::Display::fmt(error, f),
            Self::Identity(error) => std::fmt::Display::fmt(error, f),
        }
    }
}

impl std::error::Error for AgentWakeScanError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Timers(error) => Some(error),
            Self::Wake(error) => Some(error),
            Self::Identity(error) => Some(error),
        }
    }
}

impl From<AgentWakeTimerError> for AgentWakeScanError {
    fn from(error: AgentWakeTimerError) -> Self {
        Self::Timers(error)
    }
}

impl From<AgentWakeError> for AgentWakeScanError {
    fn from(error: AgentWakeError) -> Self {
        Self::Wake(error)
    }
}

impl From<AgentIdentityError> for AgentWakeScanError {
    fn from(error: AgentIdentityError) -> Self {
        Self::Identity(error)
    }
}
