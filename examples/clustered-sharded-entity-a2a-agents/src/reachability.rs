//! Peer-reachability signal from cross-node A2A run asks.
//!
//! File-discovery developer mode does not self-fence, but the routing helper
//! still records the signal so production discovery modes can consume the same
//! window later.

use std::sync::{Arc, Mutex};

use rakka_a2a::routing::A2APeerReachabilityObserver;

#[derive(Default)]
struct Window {
    attempts: u64,
    failures: u64,
}

/// Shared tracker of cross-node ask reachability.
#[derive(Clone, Default)]
pub struct PeerReachability {
    window: Arc<Mutex<Window>>,
}

impl PeerReachability {
    /// Creates an empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a cross-node ask outcome (`true` when the peer was reached).
    pub fn record(&self, reachable: bool) {
        if let Ok(mut window) = self.window.lock() {
            window.attempts += 1;
            if !reachable {
                window.failures += 1;
            }
        }
    }

    /// Evaluates and clears the current window.
    ///
    /// Returns `None` when there is no meaningful evidence.
    #[must_use]
    pub fn evaluate_and_reset(&self, member_count: usize) -> Option<bool> {
        let Ok(mut window) = self.window.lock() else {
            return None;
        };
        let attempts = window.attempts;
        let failures = window.failures;
        *window = Window::default();
        if member_count <= 1 || attempts == 0 {
            return None;
        }
        Some(failures < attempts)
    }
}

impl A2APeerReachabilityObserver for PeerReachability {
    fn record(&self, reachable: bool) {
        PeerReachability::record(self, reachable);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_attempts_or_single_node_is_neutral() {
        let reachability = PeerReachability::new();
        assert_eq!(reachability.evaluate_and_reset(2), None);
        reachability.record(false);
        assert_eq!(reachability.evaluate_and_reset(1), None);
    }

    #[test]
    fn all_failures_report_unreachable_then_reset() {
        let reachability = PeerReachability::new();
        reachability.record(false);
        reachability.record(false);
        assert_eq!(reachability.evaluate_and_reset(2), Some(false));
        assert_eq!(reachability.evaluate_and_reset(2), None);
    }

    #[test]
    fn any_success_reports_reachable() {
        let reachability = PeerReachability::new();
        reachability.record(false);
        reachability.record(true);
        assert_eq!(reachability.evaluate_and_reset(2), Some(true));
    }
}
