//! Owner routing abstraction between public ingress and sharded run owners.

use async_trait::async_trait;

use crate::error::RakkaA2AHandlerError;
use crate::protocol::{A2ARunRequest, A2ARunResponse};

/// Routes owner-only A2A run requests to the task's shard owner.
///
/// The `sharding` feature provides the cluster-backed implementation
/// (`A2ARunRouter`); tests may supply fakes. Routing rides Rakka remoting,
/// which is at-most-once: callers must treat failures as retryable per the
/// returned failure class, never as proof the owner did not act.
#[async_trait]
pub trait A2ARunRoute: Send + Sync + 'static {
    /// Routes one remote-safe owner request.
    async fn route(&self, request: A2ARunRequest) -> Result<A2ARunResponse, RakkaA2AHandlerError>;

    /// True when this node currently owns the task's shard, so the local
    /// projection watcher already observes every appended event. Ownership
    /// moves with rebalances; callers re-check per use.
    fn local_node_owns(&self, task_id: &str) -> bool;
}

/// Observes cross-node owner-ask reachability for self-fencing.
///
/// The sharding router records each remote owner ask as reachable or not so
/// an application's discovery/self-fencing layer can consume the signal.
/// The default ([`NoopPeerReachabilityObserver`]) ignores it.
pub trait A2APeerReachabilityObserver: Send + Sync + 'static {
    /// Records one cross-node ask outcome (`true` when the peer was reached).
    fn record(&self, reachable: bool);
}

/// Ignores reachability signals (default).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopPeerReachabilityObserver;

impl A2APeerReachabilityObserver for NoopPeerReachabilityObserver {
    fn record(&self, _reachable: bool) {}
}

/// Node-level drain state for Kubernetes-style graceful shutdown.
///
/// Injectable so applications can flip it from a preStop hook or drain
/// endpoint and wire the same gate into readiness probes; reads stay
/// available while mutating public ingress is refused.
#[derive(Debug, Clone)]
pub struct A2ADrainGate {
    accepting: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl A2ADrainGate {
    /// Creates a gate that accepts public commands.
    #[must_use]
    pub fn new() -> Self {
        Self {
            accepting: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }

    /// Closes mutating public ingress on this node.
    pub fn begin_drain(&self) {
        self.accepting
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Returns whether mutating public ingress is still accepted.
    #[must_use]
    pub fn accepts_public_commands(&self) -> bool {
        self.accepting.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Default for A2ADrainGate {
    fn default() -> Self {
        Self::new()
    }
}
