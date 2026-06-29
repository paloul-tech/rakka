//! Peer-reachability self-fencing.
//!
//! When membership comes from a consistent external arbiter (etcd / Kubernetes),
//! arbiter liveness is not the same as peer reachability: a node can keep its
//! registration alive yet be unable to talk to peers over `rakka-remote`, so
//! ownership keeps routing work to it and the asks time out. The fix that keeps a
//! single arbiter is for such a node to fence *itself* — drop its external
//! registration / fail readiness — turning a reachability fault into a consistent
//! membership change.
//!
//! [`SelfFenceDetector`] is the policy core of that mechanism. It consumes
//! periodic peer-reachability observations and applies **hysteresis** so a node
//! only fences after sustained unreachability and only rejoins after sustained
//! recovery, never flapping on a single bad observation. The detector decides;
//! the caller actuates (for example by revoking its etcd lease) and supplies the
//! reachability predicate (for example "reached at least one other up-member").
//!
//! Self-fencing must never directly edit the shard-ownership up-set — doing so
//! would make independent nodes disagree on owners. It only changes whether *this*
//! node stays registered. See `docs/rakka-cluster-coordination-strategy.md`.

use std::time::Duration;

/// Local node health as judged by its ability to reach peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfHealth {
    /// The node can reach peers and should remain a cluster member.
    Healthy,
    /// The node has been unable to reach peers and should fence itself
    /// (drop its external registration / fail readiness).
    Fenced,
}

impl SelfHealth {
    /// Returns true when the node should fence itself out of the cluster.
    #[must_use]
    pub const fn is_fenced(self) -> bool {
        matches!(self, Self::Fenced)
    }
}

/// Hysteresis thresholds for [`SelfFenceDetector`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelfFenceConfig {
    fence_after: Duration,
    rejoin_after: Duration,
}

impl SelfFenceConfig {
    /// Creates a configuration from the fence and rejoin hysteresis windows.
    ///
    /// `fence_after` is the sustained duration of peer-unreachability required
    /// before fencing; `rejoin_after` is the sustained duration of peer
    /// reachability required before clearing the fence. Both should exceed the
    /// observation interval so a single observation cannot flip the state.
    #[must_use]
    pub const fn new(fence_after: Duration, rejoin_after: Duration) -> Self {
        Self {
            fence_after,
            rejoin_after,
        }
    }

    /// Sustained unreachability required before the node fences itself.
    #[must_use]
    pub const fn fence_after(&self) -> Duration {
        self.fence_after
    }

    /// Sustained reachability required before the node clears its fence.
    #[must_use]
    pub const fn rejoin_after(&self) -> Duration {
        self.rejoin_after
    }
}

impl Default for SelfFenceConfig {
    /// Fences after 15s of sustained unreachability and rejoins after 10s of
    /// sustained reachability — a conservative default for an etcd/Kubernetes
    /// lease TTL on the order of 10–15s.
    fn default() -> Self {
        Self::new(Duration::from_secs(15), Duration::from_secs(10))
    }
}

/// Hysteretic self-fencing decision engine driven by peer-reachability
/// observations.
///
/// Feed one observation per poll with [`observe`](Self::observe); the returned
/// [`SelfHealth`] reflects the decision after hysteresis. The caller supplies time
/// (`now_millis`), keeping the detector deterministic and scheduler-agnostic, and
/// defines what "reachable" means for its deployment.
#[derive(Debug, Clone)]
pub struct SelfFenceDetector {
    config: SelfFenceConfig,
    health: SelfHealth,
    unreachable_since: Option<u64>,
    reachable_since: Option<u64>,
}

impl SelfFenceDetector {
    /// Creates a detector that starts [`SelfHealth::Healthy`].
    #[must_use]
    pub const fn new(config: SelfFenceConfig) -> Self {
        Self {
            config,
            health: SelfHealth::Healthy,
            unreachable_since: None,
            reachable_since: None,
        }
    }

    /// Current health decision.
    #[must_use]
    pub const fn health(&self) -> SelfHealth {
        self.health
    }

    /// Returns true when the node should currently fence itself.
    #[must_use]
    pub const fn is_fenced(&self) -> bool {
        self.health.is_fenced()
    }

    /// Configuration in use.
    #[must_use]
    pub const fn config(&self) -> &SelfFenceConfig {
        &self.config
    }

    /// Records a peer-reachability observation at `now_millis` and returns the
    /// health after applying hysteresis.
    ///
    /// `peers_reachable` is the caller's predicate (for example "reached at least
    /// one other up-member"). A single-node cluster with no peers to reach should
    /// report `true` so it never fences itself.
    pub fn observe(&mut self, now_millis: u64, peers_reachable: bool) -> SelfHealth {
        match self.health {
            SelfHealth::Healthy => {
                if peers_reachable {
                    self.unreachable_since = None;
                } else {
                    let since = *self.unreachable_since.get_or_insert(now_millis);
                    if now_millis.saturating_sub(since) >= millis(self.config.fence_after) {
                        self.health = SelfHealth::Fenced;
                        self.unreachable_since = None;
                        self.reachable_since = None;
                    }
                }
            }
            SelfHealth::Fenced => {
                if peers_reachable {
                    let since = *self.reachable_since.get_or_insert(now_millis);
                    if now_millis.saturating_sub(since) >= millis(self.config.rejoin_after) {
                        self.health = SelfHealth::Healthy;
                        self.unreachable_since = None;
                        self.reachable_since = None;
                    }
                } else {
                    self.reachable_since = None;
                }
            }
        }
        self.health
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detector() -> SelfFenceDetector {
        SelfFenceDetector::new(SelfFenceConfig::new(
            Duration::from_secs(15),
            Duration::from_secs(10),
        ))
    }

    #[test]
    fn sustained_unreachability_fences_after_window() {
        let mut detector = detector();
        assert_eq!(detector.observe(0, false), SelfHealth::Healthy);
        assert_eq!(detector.observe(10_000, false), SelfHealth::Healthy);
        // 15s after the streak began.
        assert_eq!(detector.observe(15_000, false), SelfHealth::Fenced);
        assert!(detector.is_fenced());
    }

    #[test]
    fn intermittent_reachability_resets_the_fence_streak() {
        let mut detector = detector();
        detector.observe(0, false);
        detector.observe(14_000, false);
        // A single good observation before the window resets the streak.
        assert_eq!(detector.observe(14_500, true), SelfHealth::Healthy);
        // The clock must restart; an observation at 20s is only 0ms into a new streak.
        assert_eq!(detector.observe(20_000, false), SelfHealth::Healthy);
        assert_eq!(detector.observe(35_000, false), SelfHealth::Fenced);
    }

    #[test]
    fn sustained_recovery_rejoins_after_window() {
        let mut detector = detector();
        detector.observe(0, false);
        detector.observe(15_000, false);
        assert!(detector.is_fenced());
        // Recovery must be sustained for rejoin_after (10s).
        assert_eq!(detector.observe(16_000, true), SelfHealth::Fenced);
        assert_eq!(detector.observe(26_000, true), SelfHealth::Healthy);
    }

    #[test]
    fn intermittent_recovery_does_not_rejoin() {
        let mut detector = detector();
        detector.observe(0, false);
        detector.observe(15_000, false);
        assert!(detector.is_fenced());
        detector.observe(16_000, true);
        // A bad observation resets the recovery streak; the clock restarts.
        assert_eq!(detector.observe(17_000, false), SelfHealth::Fenced);
        assert_eq!(detector.observe(26_000, true), SelfHealth::Fenced);
        assert_eq!(detector.observe(35_000, false), SelfHealth::Fenced);
        // A fresh, sustained recovery streak (36s..46s) finally rejoins.
        assert_eq!(detector.observe(36_000, true), SelfHealth::Fenced);
        assert_eq!(detector.observe(46_000, true), SelfHealth::Healthy);
    }

    #[test]
    fn single_node_reporting_reachable_never_fences() {
        let mut detector = detector();
        for tick in 0..100 {
            assert_eq!(detector.observe(tick * 1_000, true), SelfHealth::Healthy);
        }
    }
}
