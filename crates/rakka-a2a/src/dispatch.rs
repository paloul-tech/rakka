//! Push webhook dispatch boundary.
//!
//! `rakka-a2a` schedules push notifications as durable outbox effects; the
//! actual webhook delivery is an at-least-once side effect that leaves the
//! durable boundary. This module defines the [`A2APushDispatcher`] the
//! application implements to send those webhooks, the bounded delivery it
//! receives (derived from a durable notification effect), and a coordinator
//! that records retry/exhaustion visibility.
//!
//! Boundary rules:
//! - The delivery never carries resolved credentials. It carries the tenant,
//!   task, and config id so the application can resolve auth from its own
//!   credential binding at send time (DN-4).
//! - Delivery is at-least-once: effects reuse stable idempotency keys, so the
//!   webhook target must deduplicate on `idempotency_key` for effective
//!   exactly-once processing.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rakka_agent_workflow::{AgentEffect, AgentEffectKind};

use crate::push::{
    PUSH_ATTR_CONFIG_ID, PUSH_ATTR_EVENT_KIND, PUSH_ATTR_REDACTION, PUSH_ATTR_SEQUENCE,
    PUSH_ATTR_TASK_ID, PUSH_ATTR_TASK_STATE, PUSH_ATTR_TENANT, PUSH_EFFECT_TARGET_NAME,
    PUSH_EFFECT_TARGET_TYPE,
};

/// One bounded push webhook delivery derived from a durable notification
/// effect.
///
/// Contains no secret material: the application resolves auth for `config_id`
/// under `tenant` from its own credential binding when sending.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct A2APushDelivery {
    /// Owning tenant.
    pub tenant: String,
    /// Task id the notification is for.
    pub task_id: String,
    /// Push config id selecting the webhook and its credential binding.
    pub config_id: String,
    /// Task-event sequence that produced this delivery.
    pub sequence: u64,
    /// Callback URL to POST to.
    pub url: String,
    /// Stable idempotency key; the target must dedupe on it.
    pub idempotency_key: String,
    /// Public task-event kind label.
    pub task_event_kind: String,
    /// Public task-state label.
    pub task_state: String,
    /// Redaction label for the event payload.
    pub redaction: String,
}

impl A2APushDelivery {
    /// Derives a delivery from a durable effect, or `None` when the effect is
    /// not an A2A push notification.
    #[must_use]
    pub fn from_effect(effect: &AgentEffect) -> Option<Self> {
        if effect.kind != AgentEffectKind::Notification
            || effect.target.target_type != PUSH_EFFECT_TARGET_TYPE
            || effect.target.name != PUSH_EFFECT_TARGET_NAME
        {
            return None;
        }
        let attributes = &effect.target.attributes;
        Some(Self {
            tenant: attributes
                .get(PUSH_ATTR_TENANT)
                .cloned()
                .unwrap_or_default(),
            task_id: attributes
                .get(PUSH_ATTR_TASK_ID)
                .cloned()
                .unwrap_or_default(),
            config_id: attributes
                .get(PUSH_ATTR_CONFIG_ID)
                .cloned()
                .unwrap_or_default(),
            sequence: attributes
                .get(PUSH_ATTR_SEQUENCE)
                .and_then(|value| value.parse().ok())
                .unwrap_or(0),
            url: effect.target.address.clone().unwrap_or_default(),
            idempotency_key: effect.idempotency_key.as_str().to_string(),
            task_event_kind: attributes
                .get(PUSH_ATTR_EVENT_KIND)
                .cloned()
                .unwrap_or_default(),
            task_state: attributes
                .get(PUSH_ATTR_TASK_STATE)
                .cloned()
                .unwrap_or_default(),
            redaction: attributes
                .get(PUSH_ATTR_REDACTION)
                .cloned()
                .unwrap_or_default(),
        })
    }
}

/// Outcome of one push webhook delivery attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum A2APushDeliveryOutcome {
    /// The webhook target accepted the delivery.
    Delivered,
    /// Delivery failed transiently and should be retried.
    Retry {
        /// Bounded, non-secret reason for observability.
        reason: String,
    },
    /// Delivery failed permanently or exhausted its retry budget.
    Exhausted {
        /// Bounded, non-secret reason for observability.
        reason: String,
    },
}

/// Application boundary that sends A2A push webhooks from durable effects.
///
/// Implementations resolve auth for the delivery's `config_id`/`tenant` from
/// their own credential storage. The crate never provides a concrete HTTP
/// client so applications control their TLS provider, timeouts, and retries.
#[async_trait]
pub trait A2APushDispatcher: Send + Sync + 'static {
    /// Attempts one webhook delivery.
    async fn deliver(&self, delivery: &A2APushDelivery) -> A2APushDeliveryOutcome;
}

/// Bounded push-delivery counters for operational snapshots.
///
/// Observability only: never a correctness source. Contains no task ids,
/// URLs, payloads, or secrets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct A2APushDispatchSnapshot {
    /// Total delivery attempts.
    pub attempts: u64,
    /// Deliveries accepted by the webhook target.
    pub delivered: u64,
    /// Deliveries that asked to be retried.
    pub retried: u64,
    /// Deliveries that exhausted retries or failed permanently.
    pub exhausted: u64,
}

#[derive(Debug, Default)]
struct DispatchMetrics {
    snapshot: A2APushDispatchSnapshot,
}

/// Wraps an [`A2APushDispatcher`], recording bounded retry/exhaustion metrics.
///
/// The coordinator does not own the durable outbox lifecycle: a durable
/// dispatcher (for example the agent-workflow dispatcher fleet) reads due
/// notification effects, calls [`A2APushDispatchCoordinator::dispatch`], and
/// completes or re-schedules the effect based on the returned outcome.
#[derive(Clone)]
pub struct A2APushDispatchCoordinator {
    dispatcher: Arc<dyn A2APushDispatcher>,
    metrics: Arc<Mutex<DispatchMetrics>>,
}

impl std::fmt::Debug for A2APushDispatchCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("A2APushDispatchCoordinator")
            .field("metrics", &self.snapshot())
            .finish_non_exhaustive()
    }
}

impl A2APushDispatchCoordinator {
    /// Wraps a dispatcher.
    #[must_use]
    pub fn new(dispatcher: Arc<dyn A2APushDispatcher>) -> Self {
        Self {
            dispatcher,
            metrics: Arc::new(Mutex::new(DispatchMetrics::default())),
        }
    }

    /// Dispatches one delivery and records its outcome.
    pub async fn dispatch(&self, delivery: &A2APushDelivery) -> A2APushDeliveryOutcome {
        let outcome = self.dispatcher.deliver(delivery).await;
        let mut metrics = self.metrics.lock().expect("dispatch metrics mutex");
        metrics.snapshot.attempts = metrics.snapshot.attempts.saturating_add(1);
        match &outcome {
            A2APushDeliveryOutcome::Delivered => {
                metrics.snapshot.delivered = metrics.snapshot.delivered.saturating_add(1);
            }
            A2APushDeliveryOutcome::Retry { .. } => {
                metrics.snapshot.retried = metrics.snapshot.retried.saturating_add(1);
            }
            A2APushDeliveryOutcome::Exhausted { .. } => {
                metrics.snapshot.exhausted = metrics.snapshot.exhausted.saturating_add(1);
            }
        }
        outcome
    }

    /// Dispatches one due effect if it is an A2A push notification.
    ///
    /// Returns `None` when the effect is not an A2A push effect (the caller
    /// leaves such effects to other dispatchers).
    pub async fn dispatch_effect(&self, effect: &AgentEffect) -> Option<A2APushDeliveryOutcome> {
        let delivery = A2APushDelivery::from_effect(effect)?;
        Some(self.dispatch(&delivery).await)
    }

    /// Current bounded delivery metrics.
    #[must_use]
    pub fn snapshot(&self) -> A2APushDispatchSnapshot {
        self.metrics
            .lock()
            .expect("dispatch metrics mutex")
            .snapshot
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::push::push_effect;
    use crate::task::{A2ATaskEvent, A2ATaskEventPayload};
    use a2a::TaskPushNotificationConfig;
    use rakka_agent_workflow::AgentTimestampMillis;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn config() -> TaskPushNotificationConfig {
        TaskPushNotificationConfig {
            url: "https://example.com/hook".to_string(),
            id: Some("cfg-1".to_string()),
            task_id: "task-1".to_string(),
            token: None,
            authentication: None,
            tenant: Some("tenant-a".to_string()),
        }
    }

    fn terminal_event() -> A2ATaskEvent {
        A2ATaskEvent::new(
            "tenant-a",
            "task-1",
            "ctx",
            7,
            AgentTimestampMillis::new(20),
            A2ATaskEventPayload::Terminal {
                state: a2a::TaskState::Completed,
            },
        )
    }

    #[test]
    fn delivery_maps_from_effect_without_secrets() {
        let effect = push_effect(&terminal_event(), &config()).expect("effect");
        let delivery = A2APushDelivery::from_effect(&effect).expect("delivery");
        assert_eq!(delivery.tenant, "tenant-a");
        assert_eq!(delivery.task_id, "task-1");
        assert_eq!(delivery.config_id, "cfg-1");
        assert_eq!(delivery.sequence, 7);
        assert_eq!(delivery.url, "https://example.com/hook");
        assert_eq!(delivery.idempotency_key, "a2a-push:task-1:7:cfg-1");
        assert_eq!(delivery.task_event_kind, "terminal");
        assert_eq!(delivery.task_state, "completed");

        let serialized = serde_json::to_string(&effect).expect("effect json");
        assert!(
            !serialized.contains("secret") && !serialized.contains("token"),
            "effect must carry no secret material"
        );
    }

    #[test]
    fn non_push_effects_are_ignored() {
        let mut effect = push_effect(&terminal_event(), &config()).expect("effect");
        effect.kind = AgentEffectKind::ToolCall;
        assert!(A2APushDelivery::from_effect(&effect).is_none());
    }

    #[tokio::test]
    async fn coordinator_records_retry_and_exhaustion_metrics() {
        struct FlakyDispatcher {
            calls: AtomicUsize,
        }
        #[async_trait]
        impl A2APushDispatcher for FlakyDispatcher {
            async fn deliver(&self, _delivery: &A2APushDelivery) -> A2APushDeliveryOutcome {
                match self.calls.fetch_add(1, Ordering::SeqCst) {
                    0 => A2APushDeliveryOutcome::Retry {
                        reason: "connection reset".to_string(),
                    },
                    1 => A2APushDeliveryOutcome::Delivered,
                    _ => A2APushDeliveryOutcome::Exhausted {
                        reason: "gave up".to_string(),
                    },
                }
            }
        }

        let coordinator = A2APushDispatchCoordinator::new(Arc::new(FlakyDispatcher {
            calls: AtomicUsize::new(0),
        }));
        let effect = push_effect(&terminal_event(), &config()).expect("effect");

        assert!(matches!(
            coordinator.dispatch_effect(&effect).await,
            Some(A2APushDeliveryOutcome::Retry { .. })
        ));
        assert!(matches!(
            coordinator.dispatch_effect(&effect).await,
            Some(A2APushDeliveryOutcome::Delivered)
        ));
        assert!(matches!(
            coordinator.dispatch_effect(&effect).await,
            Some(A2APushDeliveryOutcome::Exhausted { .. })
        ));

        let snapshot = coordinator.snapshot();
        assert_eq!(snapshot.attempts, 3);
        assert_eq!(snapshot.delivered, 1);
        assert_eq!(snapshot.retried, 1);
        assert_eq!(snapshot.exhausted, 1);
    }
}
