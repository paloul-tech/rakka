//! Security and operational defaults shared by Rakka examples and deployments.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Default actor ask timeout used by examples and operational profiles.
pub const DEFAULT_ACTOR_ASK_TIMEOUT: Duration = Duration::from_secs(5);

/// Default remote TCP connect timeout.
pub const DEFAULT_REMOTE_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// Default remote TCP idle timeout.
pub const DEFAULT_REMOTE_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default bounded outbound queue capacity per remote peer.
pub const DEFAULT_REMOTE_OUTBOUND_QUEUE_CAPACITY: usize = 1024;

/// Default stream drain timeout used by adapter and Kubernetes examples.
pub const DEFAULT_STREAM_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Default child process startup timeout.
pub const DEFAULT_PROCESS_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Default child process graceful shutdown timeout.
pub const DEFAULT_PROCESS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Default Kubernetes pre-stop drain timeout budget.
pub const DEFAULT_KUBERNETES_PRESTOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Default Kubernetes pod termination grace period used by examples.
pub const DEFAULT_KUBERNETES_TERMINATION_GRACE_PERIOD_SECONDS: u64 = 45;

/// Deployment profile used to choose conservative defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeploymentProfile {
    /// Single-machine development with loopback-only remoting.
    Development,
    /// Local kind/minikube style cluster using pod networking.
    LocalCluster,
    /// Production-like Kubernetes deployment.
    ProductionLike,
}

impl DeploymentProfile {
    /// Stable profile label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Development => "development",
            Self::LocalCluster => "local-cluster",
            Self::ProductionLike => "production-like",
        }
    }
}

/// Security defaults that should be explicit at application boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityDefaults {
    profile: DeploymentProfile,
    remoting_bind_host: String,
    remoting_requires_registered_peers: bool,
    remoting_is_public_api: bool,
    process_requires_executable_allowlist: bool,
    process_inherits_environment_by_default: bool,
    public_http_bind_host: String,
    public_grpc_bind_host: String,
}

impl SecurityDefaults {
    /// Development defaults: remoting and public adapters bind to loopback.
    #[must_use]
    pub fn development() -> Self {
        Self {
            profile: DeploymentProfile::Development,
            remoting_bind_host: "127.0.0.1".to_owned(),
            remoting_requires_registered_peers: true,
            remoting_is_public_api: false,
            process_requires_executable_allowlist: true,
            process_inherits_environment_by_default: false,
            public_http_bind_host: "127.0.0.1".to_owned(),
            public_grpc_bind_host: "127.0.0.1".to_owned(),
        }
    }

    /// Local-cluster defaults: bind inside pod networking and rely on known peers.
    #[must_use]
    pub fn local_cluster() -> Self {
        Self {
            profile: DeploymentProfile::LocalCluster,
            remoting_bind_host: "0.0.0.0".to_owned(),
            remoting_requires_registered_peers: true,
            remoting_is_public_api: false,
            process_requires_executable_allowlist: true,
            process_inherits_environment_by_default: false,
            public_http_bind_host: "0.0.0.0".to_owned(),
            public_grpc_bind_host: "0.0.0.0".to_owned(),
        }
    }

    /// Production-like defaults: bind in pod networking and require network policy around remoting.
    #[must_use]
    pub fn production_like() -> Self {
        Self::local_cluster().with_profile(DeploymentProfile::ProductionLike)
    }

    /// Deployment profile.
    #[must_use]
    pub const fn profile(&self) -> DeploymentProfile {
        self.profile
    }

    /// Remoting bind host.
    #[must_use]
    pub fn remoting_bind_host(&self) -> &str {
        &self.remoting_bind_host
    }

    /// Whether remote traffic must come from registered peers.
    #[must_use]
    pub const fn remoting_requires_registered_peers(&self) -> bool {
        self.remoting_requires_registered_peers
    }

    /// Whether internal Rakka remoting is intended as a public client API.
    #[must_use]
    pub const fn remoting_is_public_api(&self) -> bool {
        self.remoting_is_public_api
    }

    /// Whether child processes require explicit executable allowlists.
    #[must_use]
    pub const fn process_requires_executable_allowlist(&self) -> bool {
        self.process_requires_executable_allowlist
    }

    /// Whether child processes inherit the node environment by default.
    #[must_use]
    pub const fn process_inherits_environment_by_default(&self) -> bool {
        self.process_inherits_environment_by_default
    }

    /// Public HTTP bind host.
    #[must_use]
    pub fn public_http_bind_host(&self) -> &str {
        &self.public_http_bind_host
    }

    /// Public gRPC bind host.
    #[must_use]
    pub fn public_grpc_bind_host(&self) -> &str {
        &self.public_grpc_bind_host
    }

    fn with_profile(mut self, profile: DeploymentProfile) -> Self {
        self.profile = profile;
        self
    }
}

/// Operational timeout defaults grouped for diagnostics and config examples.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationalTimeoutDefaults {
    actor_ask: Duration,
    remote_connect: Duration,
    remote_idle: Duration,
    stream_drain: Duration,
    process_startup: Duration,
    process_shutdown: Duration,
    kubernetes_prestop: Duration,
}

impl OperationalTimeoutDefaults {
    /// Creates the default timeout set.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            actor_ask: DEFAULT_ACTOR_ASK_TIMEOUT,
            remote_connect: DEFAULT_REMOTE_CONNECT_TIMEOUT,
            remote_idle: DEFAULT_REMOTE_IDLE_TIMEOUT,
            stream_drain: DEFAULT_STREAM_DRAIN_TIMEOUT,
            process_startup: DEFAULT_PROCESS_STARTUP_TIMEOUT,
            process_shutdown: DEFAULT_PROCESS_SHUTDOWN_TIMEOUT,
            kubernetes_prestop: DEFAULT_KUBERNETES_PRESTOP_TIMEOUT,
        }
    }

    /// Actor ask timeout.
    #[must_use]
    pub const fn actor_ask(&self) -> Duration {
        self.actor_ask
    }

    /// Remote connect timeout.
    #[must_use]
    pub const fn remote_connect(&self) -> Duration {
        self.remote_connect
    }

    /// Remote idle timeout.
    #[must_use]
    pub const fn remote_idle(&self) -> Duration {
        self.remote_idle
    }

    /// Stream drain timeout.
    #[must_use]
    pub const fn stream_drain(&self) -> Duration {
        self.stream_drain
    }

    /// Process startup timeout.
    #[must_use]
    pub const fn process_startup(&self) -> Duration {
        self.process_startup
    }

    /// Process shutdown timeout.
    #[must_use]
    pub const fn process_shutdown(&self) -> Duration {
        self.process_shutdown
    }

    /// Kubernetes pre-stop drain timeout.
    #[must_use]
    pub const fn kubernetes_prestop(&self) -> Duration {
        self.kubernetes_prestop
    }
}

impl Default for OperationalTimeoutDefaults {
    fn default() -> Self {
        Self::new()
    }
}
