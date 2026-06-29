//! Peer-reachability signal feeding the self-fence detector.
//!
//! The ingress records the outcome of each cross-node (`rakka-remote`) ask: a
//! transport/route failure is evidence this node cannot reach the peer that owns
//! the run; a success is evidence it can. The etcd discovery loop evaluates the
//! window each tick and feeds `rakka::cluster::SelfFenceDetector`. Local asks and
//! single-node clusters never count as evidence, so a node only fences when it is
//! genuinely partitioned from peers under traffic.

use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Window {
    attempts: u64,
    failures: u64,
}

/// Shared, cheaply-cloneable tracker of cross-node ask reachability.
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

    /// Returns the reachability verdict for the elapsed window and resets it.
    ///
    /// `Some(true)` (reachable) or `Some(false)` (every cross-node attempt failed)
    /// when there is evidence; `None` when there is none — no cross-node attempts
    /// this window, or a single-node cluster with no peers to reach. `None` is a
    /// *neutral* signal: the caller must not feed it to the self-fence detector, so
    /// idle windows neither fence nor reset an in-progress unreachable streak.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_attempts_or_single_node_is_neutral() {
        let reachability = PeerReachability::new();
        assert_eq!(reachability.evaluate_and_reset(3), None);
        reachability.record(true);
        assert_eq!(reachability.evaluate_and_reset(1), None);
    }

    #[test]
    fn all_failures_is_unreachable_then_resets() {
        let reachability = PeerReachability::new();
        reachability.record(false);
        reachability.record(false);
        assert_eq!(reachability.evaluate_and_reset(2), Some(false));
        // The window resets, so the next evaluation has no evidence.
        assert_eq!(reachability.evaluate_and_reset(2), None);
    }

    #[test]
    fn any_success_is_reachable() {
        let reachability = PeerReachability::new();
        reachability.record(false);
        reachability.record(true);
        assert_eq!(reachability.evaluate_and_reset(2), Some(true));
    }
}
