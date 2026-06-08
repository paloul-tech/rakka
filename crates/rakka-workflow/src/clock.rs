//! Workflow clocks used by durable scheduling decisions.

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::WorkflowTimestamp;

/// Clock used by workflow state transitions.
pub trait WorkflowClock: Clone + Send + Sync + 'static {
    /// Returns the current workflow timestamp.
    fn now(&self) -> WorkflowTimestamp;
}

/// System wall-clock implementation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SystemWorkflowClock;

impl WorkflowClock for SystemWorkflowClock {
    fn now(&self) -> WorkflowTimestamp {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        WorkflowTimestamp::from_millis(millis.min(u128::from(u64::MAX)) as u64)
    }
}

/// Deterministic manually advanced workflow clock for tests.
#[derive(Debug, Clone)]
pub struct ManualWorkflowClock {
    now: Arc<Mutex<WorkflowTimestamp>>,
}

impl ManualWorkflowClock {
    /// Creates a manual clock at the provided timestamp.
    #[must_use]
    pub fn new(now: WorkflowTimestamp) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    /// Sets the current timestamp.
    pub fn set(&self, now: WorkflowTimestamp) {
        *self.now.lock().expect("workflow clock mutex poisoned") = now;
    }

    /// Advances the current timestamp by milliseconds.
    pub fn advance_millis(&self, millis: u64) {
        let mut now = self.now.lock().expect("workflow clock mutex poisoned");
        *now = now.add_millis(millis);
    }
}

impl WorkflowClock for ManualWorkflowClock {
    fn now(&self) -> WorkflowTimestamp {
        *self.now.lock().expect("workflow clock mutex poisoned")
    }
}
