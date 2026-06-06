#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! Kubernetes integration foundation.

use rakka_cluster::{
    ClusterNode, ClusterProtocol, ClusterResult, DiscoveryProvider, DiscoverySnapshot, NodeAddress,
    NodeId,
};
use rakka_core::Subsystem;
use serde::{Deserialize, Serialize};

/// Crate name used in diagnostics.
pub const CRATE_NAME: &str = "rakka-k8s";

/// Default Kubernetes readiness endpoint.
pub const DEFAULT_READINESS_PATH: &str = "/ready";

/// Default Kubernetes liveness endpoint.
pub const DEFAULT_LIVENESS_PATH: &str = "/live";

/// Subsystem associated with Kubernetes integration.
#[must_use]
pub const fn subsystem() -> Subsystem {
    Subsystem::K8s
}

/// Kubernetes pod identity used to derive a Rakka node incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KubernetesPodIdentity {
    pod_name: String,
    pod_uid: String,
}

impl KubernetesPodIdentity {
    /// Creates a Kubernetes pod identity.
    #[must_use]
    pub fn new(pod_name: impl Into<String>, pod_uid: impl Into<String>) -> Self {
        Self {
            pod_name: pod_name.into(),
            pod_uid: pod_uid.into(),
        }
    }

    /// Kubernetes pod name.
    #[must_use]
    pub fn pod_name(&self) -> &str {
        &self.pod_name
    }

    /// Kubernetes pod uid, used as the node incarnation id.
    #[must_use]
    pub fn pod_uid(&self) -> &str {
        &self.pod_uid
    }

    /// Converts this pod identity to a Rakka node id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        NodeId::new(self.pod_name.clone(), self.pod_uid.clone())
    }
}

/// DNS discovery configuration for a Kubernetes headless service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KubernetesDnsDiscoveryConfig {
    namespace: String,
    service_name: String,
    cluster_domain: String,
    remoting_port: u16,
}

impl KubernetesDnsDiscoveryConfig {
    /// Creates a headless-service DNS discovery config.
    #[must_use]
    pub fn new(
        namespace: impl Into<String>,
        service_name: impl Into<String>,
        remoting_port: u16,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            service_name: service_name.into(),
            cluster_domain: "cluster.local".to_string(),
            remoting_port,
        }
    }

    /// Kubernetes namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Headless service name used for direct pod DNS.
    #[must_use]
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// Kubernetes DNS cluster domain.
    #[must_use]
    pub fn cluster_domain(&self) -> &str {
        &self.cluster_domain
    }

    /// Rakka remoting port.
    #[must_use]
    pub const fn remoting_port(&self) -> u16 {
        self.remoting_port
    }

    /// Overrides the Kubernetes cluster domain.
    #[must_use]
    pub fn with_cluster_domain(mut self, cluster_domain: impl Into<String>) -> Self {
        self.cluster_domain = cluster_domain.into();
        self
    }

    /// Builds the direct DNS host for a pod behind the headless service.
    #[must_use]
    pub fn pod_host(&self, pod_name: &str) -> String {
        if self.cluster_domain.is_empty() {
            format!("{pod_name}.{}.{}.svc", self.service_name, self.namespace)
        } else {
            format!(
                "{pod_name}.{}.{}.svc.{}",
                self.service_name, self.namespace, self.cluster_domain
            )
        }
    }

    /// Builds a Rakka cluster node descriptor for a pod identity.
    #[must_use]
    pub fn node_for_pod(
        &self,
        pod: &KubernetesPodIdentity,
        protocol: ClusterProtocol,
    ) -> ClusterNode {
        ClusterNode::new(
            pod.node_id(),
            NodeAddress::new(self.pod_host(pod.pod_name()), self.remoting_port),
        )
        .with_protocol(protocol)
    }
}

/// Discovery provider that maps known pod identities to headless-service DNS nodes.
#[derive(Debug, Clone)]
pub struct KubernetesDnsDiscovery {
    config: KubernetesDnsDiscoveryConfig,
    pods: Vec<KubernetesPodIdentity>,
    protocol: ClusterProtocol,
}

impl KubernetesDnsDiscovery {
    /// Creates a Kubernetes DNS discovery provider using the default v1 protocol.
    #[must_use]
    pub fn new(
        config: KubernetesDnsDiscoveryConfig,
        pods: impl IntoIterator<Item = KubernetesPodIdentity>,
    ) -> Self {
        Self {
            config,
            pods: pods.into_iter().collect(),
            protocol: ClusterProtocol::default(),
        }
    }

    /// Sets the protocol advertised by discovered pod contact points.
    #[must_use]
    pub const fn with_protocol(mut self, protocol: ClusterProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Returns the DNS discovery configuration.
    #[must_use]
    pub const fn config(&self) -> &KubernetesDnsDiscoveryConfig {
        &self.config
    }

    /// Returns configured pod identities.
    #[must_use]
    pub fn pods(&self) -> &[KubernetesPodIdentity] {
        &self.pods
    }
}

impl DiscoveryProvider for KubernetesDnsDiscovery {
    fn provider_name(&self) -> &str {
        "kubernetes-dns"
    }

    fn discover(&self, observed_at_millis: u64) -> ClusterResult<DiscoverySnapshot> {
        let nodes = self
            .pods
            .iter()
            .map(|pod| self.config.node_for_pod(pod, self.protocol));
        Ok(DiscoverySnapshot::new(
            self.provider_name(),
            observed_at_millis,
            nodes,
        ))
    }
}
