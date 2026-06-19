//! Kubernetes startup and readiness helpers for agent workflows.
//!
//! The lower-level `rakka-k8s` health model already knows how to fail
//! readiness while a node is joining, incompatible, draining, or missing named
//! services. This module defines the agent-workflow service names and an
//! ordered startup checklist that applications can drive as each dependency is
//! initialized.

use std::collections::BTreeSet;

use rakka_k8s::{KubernetesHealthSnapshot, KubernetesNodeHealth, KubernetesProbeSnapshot};
use serde::{Deserialize, Serialize};

/// Readiness service name for OpenTelemetry resource configuration.
pub const AGENT_WORKFLOW_STARTUP_TELEMETRY_RESOURCE: &str = "telemetry-resource";

/// Readiness service name for the OTLP exporter or local bridge.
pub const AGENT_WORKFLOW_STARTUP_OTLP_EXPORTER: &str = "otlp-exporter";

/// Readiness service name for PostgreSQL connectivity.
pub const AGENT_WORKFLOW_STARTUP_POSTGRES: &str = "postgres";

/// Readiness service name for durable run-state stores.
pub const AGENT_WORKFLOW_STARTUP_DURABLE_STATE: &str = "durable-state";

/// Readiness service name for operational query indexes.
pub const AGENT_WORKFLOW_STARTUP_QUERY_INDEX: &str = "query-index";

/// Readiness service name for artifact storage.
pub const AGENT_WORKFLOW_STARTUP_ARTIFACT_STORE: &str = "artifact-store";

/// Readiness service name for actor-system initialization.
pub const AGENT_WORKFLOW_STARTUP_ACTOR_SYSTEM: &str = "actor-system";

/// Readiness service name for internal Rakka remoting.
pub const AGENT_WORKFLOW_STARTUP_REMOTING: &str = "remoting";

/// Readiness service name for cluster sharding registration.
pub const AGENT_WORKFLOW_STARTUP_SHARDING: &str = "sharding";

/// Readiness service name for agent workflow definition registration.
pub const AGENT_WORKFLOW_STARTUP_WORKFLOW_REGISTRY: &str = "workflow-registry";

/// Readiness service name for operational snapshot registration.
pub const AGENT_WORKFLOW_STARTUP_OPERATIONAL_SNAPSHOTS: &str = "operational-snapshots";

/// Default startup steps expected before an agent workflow pod becomes ready.
pub const DEFAULT_AGENT_WORKFLOW_STARTUP_STEPS: [AgentWorkflowStartupStep; 11] = [
    AgentWorkflowStartupStep::TelemetryResource,
    AgentWorkflowStartupStep::OtlpExporter,
    AgentWorkflowStartupStep::Postgres,
    AgentWorkflowStartupStep::DurableState,
    AgentWorkflowStartupStep::QueryIndex,
    AgentWorkflowStartupStep::ArtifactStore,
    AgentWorkflowStartupStep::ActorSystem,
    AgentWorkflowStartupStep::Remoting,
    AgentWorkflowStartupStep::Sharding,
    AgentWorkflowStartupStep::WorkflowRegistry,
    AgentWorkflowStartupStep::OperationalSnapshots,
];

/// One ordered startup requirement for Kubernetes readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentWorkflowStartupStep {
    /// OpenTelemetry resource attributes have been configured.
    TelemetryResource,
    /// OTLP exporter, bridge, or collector connection configuration is ready.
    OtlpExporter,
    /// PostgreSQL connectivity and credentials have been validated.
    Postgres,
    /// Durable state stores have been connected and migration checks passed.
    DurableState,
    /// Operational query index has been connected and schema compatibility passed.
    QueryIndex,
    /// Artifact store configuration and credentials have been validated.
    ArtifactStore,
    /// Actor system has been initialized.
    ActorSystem,
    /// Internal Rakka remoting has been initialized.
    Remoting,
    /// Cluster sharding has been initialized and registrations are installed.
    Sharding,
    /// Agent workflow definitions have been registered.
    WorkflowRegistry,
    /// Operational snapshots have been registered.
    OperationalSnapshots,
}

impl AgentWorkflowStartupStep {
    /// Stable service name used by `KubernetesNodeHealth`.
    #[must_use]
    pub const fn service_name(self) -> &'static str {
        match self {
            Self::TelemetryResource => AGENT_WORKFLOW_STARTUP_TELEMETRY_RESOURCE,
            Self::OtlpExporter => AGENT_WORKFLOW_STARTUP_OTLP_EXPORTER,
            Self::Postgres => AGENT_WORKFLOW_STARTUP_POSTGRES,
            Self::DurableState => AGENT_WORKFLOW_STARTUP_DURABLE_STATE,
            Self::QueryIndex => AGENT_WORKFLOW_STARTUP_QUERY_INDEX,
            Self::ArtifactStore => AGENT_WORKFLOW_STARTUP_ARTIFACT_STORE,
            Self::ActorSystem => AGENT_WORKFLOW_STARTUP_ACTOR_SYSTEM,
            Self::Remoting => AGENT_WORKFLOW_STARTUP_REMOTING,
            Self::Sharding => AGENT_WORKFLOW_STARTUP_SHARDING,
            Self::WorkflowRegistry => AGENT_WORKFLOW_STARTUP_WORKFLOW_REGISTRY,
            Self::OperationalSnapshots => AGENT_WORKFLOW_STARTUP_OPERATIONAL_SNAPSHOTS,
        }
    }

    /// Human-oriented description of the startup requirement.
    #[must_use]
    pub const fn description(self) -> &'static str {
        match self {
            Self::TelemetryResource => "OpenTelemetry resource attributes configured",
            Self::OtlpExporter => "OTLP exporter or bridge configured",
            Self::Postgres => "PostgreSQL connectivity validated",
            Self::DurableState => "durable workflow stores connected",
            Self::QueryIndex => "operational query index connected",
            Self::ArtifactStore => "artifact store configured",
            Self::ActorSystem => "actor system initialized",
            Self::Remoting => "internal Rakka remoting initialized",
            Self::Sharding => "cluster sharding initialized",
            Self::WorkflowRegistry => "workflow definitions registered",
            Self::OperationalSnapshots => "operational snapshots registered",
        }
    }

    /// Parses a service name from `RAKKA_REQUIRED_SERVICES`.
    #[must_use]
    pub fn from_service_name(service_name: &str) -> Option<Self> {
        match service_name.trim() {
            AGENT_WORKFLOW_STARTUP_TELEMETRY_RESOURCE => Some(Self::TelemetryResource),
            AGENT_WORKFLOW_STARTUP_OTLP_EXPORTER => Some(Self::OtlpExporter),
            AGENT_WORKFLOW_STARTUP_POSTGRES => Some(Self::Postgres),
            AGENT_WORKFLOW_STARTUP_DURABLE_STATE => Some(Self::DurableState),
            AGENT_WORKFLOW_STARTUP_QUERY_INDEX => Some(Self::QueryIndex),
            AGENT_WORKFLOW_STARTUP_ARTIFACT_STORE => Some(Self::ArtifactStore),
            AGENT_WORKFLOW_STARTUP_ACTOR_SYSTEM => Some(Self::ActorSystem),
            AGENT_WORKFLOW_STARTUP_REMOTING => Some(Self::Remoting),
            AGENT_WORKFLOW_STARTUP_SHARDING => Some(Self::Sharding),
            AGENT_WORKFLOW_STARTUP_WORKFLOW_REGISTRY => Some(Self::WorkflowRegistry),
            AGENT_WORKFLOW_STARTUP_OPERATIONAL_SNAPSHOTS => Some(Self::OperationalSnapshots),
            _ => None,
        }
    }
}

/// Returns the default service names for `RAKKA_REQUIRED_SERVICES`.
#[must_use]
pub fn default_agent_workflow_required_services() -> Vec<&'static str> {
    DEFAULT_AGENT_WORKFLOW_STARTUP_STEPS
        .iter()
        .map(|step| step.service_name())
        .collect()
}

/// Parses comma-separated startup service names into known startup steps.
#[must_use]
pub fn parse_agent_workflow_required_services(value: &str) -> Vec<AgentWorkflowStartupStep> {
    value
        .split(',')
        .filter_map(AgentWorkflowStartupStep::from_service_name)
        .collect()
}

/// Agent workflow startup checklist backed by Kubernetes readiness health.
#[derive(Debug, Clone)]
pub struct AgentWorkflowKubernetesStartup {
    health: KubernetesNodeHealth,
    required_steps: BTreeSet<AgentWorkflowStartupStep>,
    completed_steps: BTreeSet<AgentWorkflowStartupStep>,
}

impl AgentWorkflowKubernetesStartup {
    /// Creates a startup checklist with the default required steps.
    #[must_use]
    pub fn new(health: KubernetesNodeHealth) -> Self {
        Self::with_required_steps(health, DEFAULT_AGENT_WORKFLOW_STARTUP_STEPS)
    }

    /// Creates a startup checklist with explicit required steps.
    #[must_use]
    pub fn with_required_steps(
        health: KubernetesNodeHealth,
        steps: impl IntoIterator<Item = AgentWorkflowStartupStep>,
    ) -> Self {
        let mut required_steps = BTreeSet::new();
        for step in steps {
            health.require_service(step.service_name());
            required_steps.insert(step);
        }
        Self {
            health,
            required_steps,
            completed_steps: BTreeSet::new(),
        }
    }

    /// Shared Kubernetes health model.
    #[must_use]
    pub const fn health(&self) -> &KubernetesNodeHealth {
        &self.health
    }

    /// Required startup steps.
    #[must_use]
    pub fn required_steps(&self) -> Vec<AgentWorkflowStartupStep> {
        self.required_steps.iter().copied().collect()
    }

    /// Completed startup steps.
    #[must_use]
    pub fn completed_steps(&self) -> Vec<AgentWorkflowStartupStep> {
        self.completed_steps.iter().copied().collect()
    }

    /// Pending startup steps.
    #[must_use]
    pub fn pending_steps(&self) -> Vec<AgentWorkflowStartupStep> {
        self.required_steps
            .difference(&self.completed_steps)
            .copied()
            .collect()
    }

    /// Marks one startup step complete and registers its readiness service.
    pub fn complete_step(&mut self, step: AgentWorkflowStartupStep) {
        if self.required_steps.contains(&step) {
            self.completed_steps.insert(step);
            self.health.register_service(step.service_name());
        }
    }

    /// Marks all required startup steps complete.
    pub fn complete_all_steps(&mut self) {
        for step in self.required_steps() {
            self.complete_step(step);
        }
    }

    /// Marks one startup step incomplete and unregisters its readiness service.
    pub fn reset_step(&mut self, step: AgentWorkflowStartupStep) {
        if self.required_steps.contains(&step) {
            self.completed_steps.remove(&step);
            self.health.unregister_service(step.service_name());
        }
    }

    /// Records that deployment compatibility checks passed.
    pub fn accept_compatibility(&self) {
        self.health.accept_compatibility();
    }

    /// Records a compatibility failure, keeping readiness failed closed.
    pub fn record_compatibility_failure(&self, message: impl Into<String>) {
        self.health.record_compatibility_failure(message);
    }

    /// Computes the current Kubernetes readiness probe.
    #[must_use]
    pub fn readiness_probe(&self) -> KubernetesProbeSnapshot {
        self.health.readiness_probe()
    }

    /// Returns an agent workflow startup snapshot with Kubernetes health.
    #[must_use]
    pub fn snapshot(&self) -> AgentWorkflowStartupSnapshot {
        AgentWorkflowStartupSnapshot {
            required_steps: self.required_steps(),
            completed_steps: self.completed_steps(),
            pending_steps: self.pending_steps(),
            readiness: self.health.readiness_probe(),
            health: self.health.snapshot(),
        }
    }
}

/// Snapshot of agent workflow startup readiness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentWorkflowStartupSnapshot {
    /// Required startup steps.
    pub required_steps: Vec<AgentWorkflowStartupStep>,
    /// Completed startup steps.
    pub completed_steps: Vec<AgentWorkflowStartupStep>,
    /// Pending startup steps.
    pub pending_steps: Vec<AgentWorkflowStartupStep>,
    /// Current Kubernetes readiness probe.
    pub readiness: KubernetesProbeSnapshot,
    /// Current Kubernetes health snapshot.
    pub health: KubernetesHealthSnapshot,
}
