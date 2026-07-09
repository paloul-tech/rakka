//! Bounded stream admission and stream metrics.

// The request handler (Slice 7.4) is the in-crate consumer of the admission
// internals; until it lands only the public settings/snapshot types are used.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

const DEFAULT_MAX_NODE_STREAMS: usize = 128;
const DEFAULT_MAX_TASK_STREAMS: usize = 32;

/// Bounded admission limits for A2A streaming subscribers on one node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct A2AStreamLimitSettings {
    /// Maximum concurrently open streams on this node.
    pub max_node_streams: usize,
    /// Maximum concurrently open streams per task on this node.
    pub max_task_streams: usize,
}

impl Default for A2AStreamLimitSettings {
    fn default() -> Self {
        Self {
            max_node_streams: DEFAULT_MAX_NODE_STREAMS,
            max_task_streams: DEFAULT_MAX_TASK_STREAMS,
        }
    }
}

/// Bounded counters describing stream admission and delivery on one node.
///
/// An operational snapshot, never a correctness source. Labels and fields
/// stay bounded: no task ids, payloads, or errors.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct A2AStreamMetricsSnapshot {
    /// Streams currently open.
    pub open_streams: usize,
    /// Total streams admitted since start.
    pub opened_streams: u64,
    /// Total streams closed since start.
    pub closed_streams: u64,
    /// Admissions rejected by node or task limits.
    pub over_limit_streams: u64,
    /// Streams ended because the subscriber lagged the event buffer.
    pub lagged_streams: u64,
    /// Streams dropped by disconnect or owner unavailability.
    pub dropped_streams: u64,
    /// Replay requests served on stream open.
    pub replay_requests: u64,
    /// Total replay latency across replay requests, in milliseconds.
    pub replay_latency_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum A2AStreamLimitError {
    NodeLimit { limit: usize },
    TaskLimit { limit: usize },
}

impl A2AStreamLimitError {
    pub(crate) const fn message(&self) -> &'static str {
        match self {
            Self::NodeLimit { .. } => "node stream limit reached; retry on another healthy node",
            Self::TaskLimit { .. } => "task stream subscriber limit reached; retry later",
        }
    }
}

#[derive(Debug, Default)]
struct A2AStreamLimitState {
    open_streams: usize,
    task_streams: BTreeMap<String, usize>,
    metrics: A2AStreamMetricsSnapshot,
}

#[derive(Debug, Clone)]
pub(crate) struct A2AStreamLimits {
    inner: Arc<Mutex<A2AStreamLimitState>>,
    settings: A2AStreamLimitSettings,
}

impl A2AStreamLimits {
    pub(crate) fn new(settings: A2AStreamLimitSettings) -> Self {
        Self {
            inner: Arc::new(Mutex::new(A2AStreamLimitState::default())),
            settings,
        }
    }

    pub(crate) fn acquire(&self, task_id: &str) -> Result<A2AStreamLease, A2AStreamLimitError> {
        let mut state = self.inner.lock().expect("stream limit mutex");
        if state.open_streams >= self.settings.max_node_streams {
            state.metrics.over_limit_streams = state.metrics.over_limit_streams.saturating_add(1);
            return Err(A2AStreamLimitError::NodeLimit {
                limit: self.settings.max_node_streams,
            });
        }
        let task_streams = state.task_streams.get(task_id).copied().unwrap_or(0);
        if task_streams >= self.settings.max_task_streams {
            state.metrics.over_limit_streams = state.metrics.over_limit_streams.saturating_add(1);
            return Err(A2AStreamLimitError::TaskLimit {
                limit: self.settings.max_task_streams,
            });
        }

        state.open_streams = state.open_streams.saturating_add(1);
        state
            .task_streams
            .insert(task_id.to_string(), task_streams.saturating_add(1));
        state.metrics.open_streams = state.open_streams;
        state.metrics.opened_streams = state.metrics.opened_streams.saturating_add(1);
        Ok(A2AStreamLease {
            task_id: task_id.to_string(),
            limits: Arc::downgrade(&self.inner),
        })
    }

    pub(crate) fn record_lagged(&self) {
        let mut state = self.inner.lock().expect("stream limit mutex");
        state.metrics.lagged_streams = state.metrics.lagged_streams.saturating_add(1);
    }

    pub(crate) fn record_dropped(&self) {
        let mut state = self.inner.lock().expect("stream limit mutex");
        state.metrics.dropped_streams = state.metrics.dropped_streams.saturating_add(1);
    }

    pub(crate) fn record_replay(&self, latency_millis: u64) {
        let mut state = self.inner.lock().expect("stream limit mutex");
        state.metrics.replay_requests = state.metrics.replay_requests.saturating_add(1);
        state.metrics.replay_latency_millis = state
            .metrics
            .replay_latency_millis
            .saturating_add(latency_millis);
    }

    pub(crate) fn snapshot(&self) -> A2AStreamMetricsSnapshot {
        self.inner
            .lock()
            .expect("stream limit mutex")
            .metrics
            .clone()
    }
}

impl Default for A2AStreamLimits {
    fn default() -> Self {
        Self::new(A2AStreamLimitSettings::default())
    }
}

#[derive(Debug)]
pub(crate) struct A2AStreamLease {
    task_id: String,
    limits: Weak<Mutex<A2AStreamLimitState>>,
}

impl Drop for A2AStreamLease {
    fn drop(&mut self) {
        let Some(inner) = self.limits.upgrade() else {
            return;
        };
        let mut state = inner.lock().expect("stream limit mutex");
        state.open_streams = state.open_streams.saturating_sub(1);
        if let Some(count) = state.task_streams.get_mut(&self.task_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.task_streams.remove(&self.task_id);
            }
        }
        state.metrics.open_streams = state.open_streams;
        state.metrics.closed_streams = state.metrics.closed_streams.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_limits_and_lease_drop_update_metrics() {
        let limits = A2AStreamLimits::new(A2AStreamLimitSettings {
            max_node_streams: 2,
            max_task_streams: 1,
        });

        let task_a = limits.acquire("task-a").expect("task-a stream");
        let task_limit = limits
            .acquire("task-a")
            .expect_err("same task should hit per-task limit");
        assert!(matches!(
            task_limit,
            A2AStreamLimitError::TaskLimit { limit: 1, .. }
        ));

        let task_b = limits.acquire("task-b").expect("task-b stream");
        let node_limit = limits
            .acquire("task-c")
            .expect_err("third stream should hit node limit");
        assert!(matches!(
            node_limit,
            A2AStreamLimitError::NodeLimit { limit: 2 }
        ));

        let snapshot = limits.snapshot();
        assert_eq!(snapshot.open_streams, 2);
        assert_eq!(snapshot.over_limit_streams, 2);

        drop(task_b);
        drop(task_a);

        let snapshot = limits.snapshot();
        assert_eq!(snapshot.open_streams, 0);
        assert_eq!(snapshot.closed_streams, 2);
    }
}
