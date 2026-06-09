//! Kubernetes readiness and liveness probe state.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use rakka_cluster::{ClusterMembership, MembershipState, NodeId};
use serde::{Deserialize, Serialize};

/// Shared probe hook that HTTP integrations can call.
pub type KubernetesProbeHook = Arc<dyn Fn() -> KubernetesProbeSnapshot + Send + Sync + 'static>;

/// Probe kind represented by a `KubernetesProbeSnapshot`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KubernetesProbeKind {
    /// Kubernetes readiness probe.
    Readiness,
    /// Kubernetes liveness probe.
    Liveness,
}

/// Kubernetes probe result with stable reason codes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KubernetesProbeSnapshot {
    kind: KubernetesProbeKind,
    passed: bool,
    reasons: Vec<String>,
}

impl KubernetesProbeSnapshot {
    /// Creates a probe snapshot.
    #[must_use]
    pub fn new(kind: KubernetesProbeKind, passed: bool, reasons: Vec<String>) -> Self {
        Self {
            kind,
            passed,
            reasons,
        }
    }

    /// Probe kind.
    #[must_use]
    pub const fn kind(&self) -> KubernetesProbeKind {
        self.kind
    }

    /// Returns true when this probe should return success to Kubernetes.
    #[must_use]
    pub const fn passed(&self) -> bool {
        self.passed
    }

    /// Stable reason codes explaining a failed probe or notable healthy state.
    #[must_use]
    pub fn reasons(&self) -> &[String] {
        &self.reasons
    }
}

/// Point-in-time Kubernetes-facing node health state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KubernetesHealthSnapshot {
    local_node_id: NodeId,
    membership_state: MembershipState,
    cluster_compatible: bool,
    compatibility_error: Option<String>,
    required_services: Vec<String>,
    registered_services: Vec<String>,
    missing_services: Vec<String>,
    draining: bool,
    runtime_stuck: Option<String>,
    rebalancing: Vec<String>,
}

impl KubernetesHealthSnapshot {
    /// Local node id.
    #[must_use]
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Current local membership state.
    #[must_use]
    pub const fn membership_state(&self) -> MembershipState {
        self.membership_state
    }

    /// Returns true after cluster protocol compatibility has been accepted.
    #[must_use]
    pub const fn cluster_compatible(&self) -> bool {
        self.cluster_compatible
    }

    /// Last protocol compatibility failure, when known.
    #[must_use]
    pub fn compatibility_error(&self) -> Option<&str> {
        self.compatibility_error.as_deref()
    }

    /// Required service names.
    #[must_use]
    pub fn required_services(&self) -> &[String] {
        &self.required_services
    }

    /// Registered service names.
    #[must_use]
    pub fn registered_services(&self) -> &[String] {
        &self.registered_services
    }

    /// Required services not yet registered.
    #[must_use]
    pub fn missing_services(&self) -> &[String] {
        &self.missing_services
    }

    /// Returns true after Kubernetes pre-stop drain begins.
    #[must_use]
    pub const fn draining(&self) -> bool {
        self.draining
    }

    /// Runtime stuck reason, when liveness should fail.
    #[must_use]
    pub fn runtime_stuck(&self) -> Option<&str> {
        self.runtime_stuck.as_deref()
    }

    /// Entity types or subsystems currently rebalancing.
    #[must_use]
    pub fn rebalancing(&self) -> &[String] {
        &self.rebalancing
    }
}

/// Shared Kubernetes-facing node health model.
#[derive(Debug, Clone)]
pub struct KubernetesNodeHealth {
    inner: Arc<Mutex<HealthState>>,
}

impl KubernetesNodeHealth {
    /// Creates a health model for the local node.
    #[must_use]
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HealthState::new(local_node_id))),
        }
    }

    /// Creates a health model from a membership table.
    #[must_use]
    pub fn from_membership(membership: &ClusterMembership) -> Self {
        let health = Self::new(membership.local_node_id().clone());
        health.refresh_from_membership(membership);
        health
    }

    /// Updates local membership state from the cluster membership table.
    pub fn refresh_from_membership(
        &self,
        membership: &ClusterMembership,
    ) -> KubernetesHealthSnapshot {
        let mut state = self.lock();
        state.membership_state = membership
            .member(membership.local_node_id())
            .map(|member| member.state())
            .unwrap_or(MembershipState::Removed);
        state.snapshot()
    }

    /// Records that cluster protocol compatibility was accepted.
    pub fn accept_compatibility(&self) {
        let mut state = self.lock();
        state.cluster_compatible = true;
        state.compatibility_error = None;
    }

    /// Records a protocol compatibility failure and makes readiness fail closed.
    pub fn record_compatibility_failure(&self, message: impl Into<String>) {
        let mut state = self.lock();
        state.cluster_compatible = false;
        state.compatibility_error = Some(message.into());
    }

    /// Adds a required service that must be registered before readiness succeeds.
    pub fn require_service(&self, service: impl Into<String>) {
        self.lock().required_services.insert(service.into());
    }

    /// Marks a service as registered and available.
    pub fn register_service(&self, service: impl Into<String>) {
        self.lock().registered_services.insert(service.into());
    }

    /// Marks a service as unavailable.
    pub fn unregister_service(&self, service: &str) {
        self.lock().registered_services.remove(service);
    }

    /// Marks the node as draining for Kubernetes pre-stop.
    pub fn begin_drain(&self) {
        self.lock().draining = true;
    }

    /// Returns true after drain has started.
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.lock().draining
    }

    /// Records a normal rebalance or handoff. Liveness remains successful.
    pub fn mark_rebalancing(&self, scope: impl Into<String>) {
        self.lock().rebalancing.insert(scope.into());
    }

    /// Clears one rebalance marker.
    pub fn clear_rebalancing(&self, scope: &str) {
        self.lock().rebalancing.remove(scope);
    }

    /// Clears all rebalance markers.
    pub fn clear_all_rebalancing(&self) {
        self.lock().rebalancing.clear();
    }

    /// Marks the runtime stuck, causing liveness to fail.
    pub fn mark_runtime_stuck(&self, message: impl Into<String>) {
        self.lock().runtime_stuck = Some(message.into());
    }

    /// Clears a runtime stuck condition.
    pub fn clear_runtime_stuck(&self) {
        self.lock().runtime_stuck = None;
    }

    /// Returns a stable state snapshot.
    #[must_use]
    pub fn snapshot(&self) -> KubernetesHealthSnapshot {
        self.lock().snapshot()
    }

    /// Computes readiness for Kubernetes.
    #[must_use]
    pub fn readiness_probe(&self) -> KubernetesProbeSnapshot {
        let state = self.lock();
        let mut reasons = Vec::new();
        if state.draining {
            reasons.push("node-draining".to_string());
        }
        if state.membership_state != MembershipState::Up {
            reasons.push(format!(
                "cluster-not-up:{}",
                membership_state_code(state.membership_state)
            ));
        }
        if !state.cluster_compatible {
            reasons.push("compatibility-not-accepted".to_string());
        }
        for service in state.missing_services() {
            reasons.push(format!("missing-service:{service}"));
        }

        KubernetesProbeSnapshot::new(KubernetesProbeKind::Readiness, reasons.is_empty(), reasons)
    }

    /// Computes liveness for Kubernetes.
    #[must_use]
    pub fn liveness_probe(&self) -> KubernetesProbeSnapshot {
        let state = self.lock();
        let mut reasons = Vec::new();
        if let Some(message) = &state.runtime_stuck {
            reasons.push(format!("runtime-stuck:{message}"));
        }

        KubernetesProbeSnapshot::new(KubernetesProbeKind::Liveness, reasons.is_empty(), reasons)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HealthState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Creates a shared readiness probe hook for HTTP handlers.
#[must_use]
pub fn readiness_probe_hook(health: KubernetesNodeHealth) -> KubernetesProbeHook {
    Arc::new(move || health.readiness_probe())
}

/// Creates a shared liveness probe hook for HTTP handlers.
#[must_use]
pub fn liveness_probe_hook(health: KubernetesNodeHealth) -> KubernetesProbeHook {
    Arc::new(move || health.liveness_probe())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HealthState {
    local_node_id: NodeId,
    membership_state: MembershipState,
    cluster_compatible: bool,
    compatibility_error: Option<String>,
    required_services: BTreeSet<String>,
    registered_services: BTreeSet<String>,
    draining: bool,
    runtime_stuck: Option<String>,
    rebalancing: BTreeSet<String>,
}

impl HealthState {
    fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            membership_state: MembershipState::Joining,
            cluster_compatible: false,
            compatibility_error: None,
            required_services: BTreeSet::new(),
            registered_services: BTreeSet::new(),
            draining: false,
            runtime_stuck: None,
            rebalancing: BTreeSet::new(),
        }
    }

    fn snapshot(&self) -> KubernetesHealthSnapshot {
        KubernetesHealthSnapshot {
            local_node_id: self.local_node_id.clone(),
            membership_state: self.membership_state,
            cluster_compatible: self.cluster_compatible,
            compatibility_error: self.compatibility_error.clone(),
            required_services: self.required_services.iter().cloned().collect(),
            registered_services: self.registered_services.iter().cloned().collect(),
            missing_services: self.missing_services(),
            draining: self.draining,
            runtime_stuck: self.runtime_stuck.clone(),
            rebalancing: self.rebalancing.iter().cloned().collect(),
        }
    }

    fn missing_services(&self) -> Vec<String> {
        self.required_services
            .difference(&self.registered_services)
            .cloned()
            .collect()
    }
}

fn membership_state_code(state: MembershipState) -> &'static str {
    match state {
        MembershipState::Joining => "joining",
        MembershipState::Up => "up",
        MembershipState::Leaving => "leaving",
        MembershipState::Unreachable => "unreachable",
        MembershipState::Down => "down",
        MembershipState::Removed => "removed",
    }
}
