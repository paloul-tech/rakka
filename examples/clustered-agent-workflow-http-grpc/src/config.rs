//! Environment-driven per-process configuration.

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use rakka::cluster::{ClusterNode, NodeAddress, NodeId};

use crate::support::{
    default_node_incarnation, default_node_logical_id, env_u16, parse_u16, ExampleError,
    ExampleResult, DEFAULT_ETCD_LEASE_TTL_SECONDS, DEFAULT_ETCD_PREFIX, DEFAULT_RAKKA_PORT,
};

/// Which cluster-membership discovery source the process uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryProviderKind {
    /// Shared file directory (local development).
    File,
    /// etcd register/lease/watch (dynamic; Kubernetes autoscaling).
    Etcd,
}

/// Which durable state backend the process uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceKind {
    /// Local file store (single host only).
    File,
    /// Shared PostgreSQL store (multi-pod recovery). Requires the `postgres`
    /// build feature.
    Postgres,
}

/// Resolved configuration for one cluster process.
#[derive(Debug, Clone)]
pub struct ExampleConfig {
    pub bind_host: IpAddr,
    pub advertise_host: String,
    pub rakka_port: u16,
    pub http_port: u16,
    pub grpc_port: u16,
    pub node_logical_id: String,
    pub node_incarnation: String,
    pub discovery_provider: DiscoveryProviderKind,
    pub discovery_dir: PathBuf,
    pub etcd_endpoints: Vec<String>,
    pub etcd_prefix: String,
    pub etcd_lease_ttl_seconds: i64,
    pub persistence: PersistenceKind,
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub postgres_dsn: Option<String>,
    pub run_state_dir: PathBuf,
    pub workflow_state_dir: PathBuf,
}

impl ExampleConfig {
    /// Builds configuration from environment variables, applying defaults so a
    /// single `cargo run` works without any setup.
    pub fn from_env() -> ExampleResult<Self> {
        let rakka_port = env_u16("RAKKA_PORT", DEFAULT_RAKKA_PORT)?;
        let http_port = env::var("RAKKA_HTTP_PORT")
            .ok()
            .map(|value| parse_u16("RAKKA_HTTP_PORT", &value))
            .transpose()?
            .unwrap_or_else(|| rakka_port.saturating_add(10_000));
        let grpc_port = env::var("RAKKA_GRPC_PORT")
            .ok()
            .map(|value| parse_u16("RAKKA_GRPC_PORT", &value))
            .transpose()?
            .unwrap_or_else(|| rakka_port.saturating_add(20_000));
        let bind_host = env::var("RAKKA_BIND_HOST")
            .unwrap_or_else(|_| Ipv4Addr::LOCALHOST.to_string())
            .parse::<IpAddr>()?;

        // In Kubernetes the downward API supplies the pod identity/address; those
        // take effect unless an explicit override is set.
        let advertise_host = first_env(&["RAKKA_ADVERTISE_HOST", "RAKKA_POD_IP"])
            .unwrap_or_else(|| bind_host.to_string());
        // The logical id is stable for an identity (pod name) so a restarted
        // process rejoins as the same logical node; the incarnation changes each
        // start (pod uid).
        let node_logical_id = first_env(&["RAKKA_NODE_LOGICAL_ID", "RAKKA_POD_NAME"])
            .unwrap_or_else(|| default_node_logical_id(rakka_port));
        let node_incarnation = first_env(&["RAKKA_NODE_INCARNATION", "RAKKA_POD_UID"])
            .unwrap_or_else(|| default_node_incarnation(rakka_port));

        let discovery_provider = match env::var("RAKKA_DISCOVERY_PROVIDER")
            .unwrap_or_else(|_| "file".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "file" => DiscoveryProviderKind::File,
            "etcd" => DiscoveryProviderKind::Etcd,
            other => {
                return Err(ExampleError::from(crate::support::example_error(format!(
                    "RAKKA_DISCOVERY_PROVIDER must be 'file' or 'etcd', got '{other}'"
                ))))
            }
        };

        let etcd_endpoints = env::var("RAKKA_ETCD_ENDPOINTS")
            .unwrap_or_else(|_| "http://127.0.0.1:2379".to_string())
            .split(',')
            .map(|endpoint| endpoint.trim().to_string())
            .filter(|endpoint| !endpoint.is_empty())
            .collect::<Vec<_>>();
        let etcd_prefix =
            env::var("RAKKA_ETCD_PREFIX").unwrap_or_else(|_| DEFAULT_ETCD_PREFIX.to_string());
        let etcd_lease_ttl_seconds = env::var("RAKKA_ETCD_LEASE_TTL_SECONDS")
            .ok()
            .map(|value| {
                value.parse::<i64>().map_err(|error| {
                    ExampleError::from(crate::support::example_error(format!(
                        "RAKKA_ETCD_LEASE_TTL_SECONDS must be an integer: {error}"
                    )))
                })
            })
            .transpose()?
            .unwrap_or(DEFAULT_ETCD_LEASE_TTL_SECONDS);

        let persistence = match env::var("RAKKA_PERSISTENCE")
            .unwrap_or_else(|_| "file".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "file" => PersistenceKind::File,
            "postgres" => PersistenceKind::Postgres,
            other => {
                return Err(ExampleError::from(crate::support::example_error(format!(
                    "RAKKA_PERSISTENCE must be 'file' or 'postgres', got '{other}'"
                ))))
            }
        };
        let postgres_dsn = env::var("RAKKA_POSTGRES_DSN").ok();

        let base_dir = env::temp_dir().join("rakka-clustered-agent-workflow-http");
        let discovery_dir = env::var_os("RAKKA_DISCOVERY_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| base_dir.join("discovery"));
        let state_base = env::var_os("RAKKA_STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| base_dir.join("state"));

        Ok(Self {
            bind_host,
            advertise_host,
            rakka_port,
            http_port,
            grpc_port,
            node_logical_id,
            node_incarnation,
            discovery_provider,
            discovery_dir,
            etcd_endpoints,
            etcd_prefix,
            etcd_lease_ttl_seconds,
            persistence,
            postgres_dsn,
            run_state_dir: state_base.join("runs"),
            workflow_state_dir: state_base.join("workflow"),
        })
    }

    /// Stable cluster identity for this process used by membership and ownership.
    pub fn local_node(&self) -> ClusterNode {
        ClusterNode::new(
            NodeId::new(self.node_logical_id.clone(), self.node_incarnation.clone()),
            NodeAddress::new(self.advertise_host.clone(), self.rakka_port),
        )
        .with_role("agent-workflow")
    }

    /// Address the HTTP ingress server binds locally.
    pub fn http_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.http_port)
    }

    /// Address the gRPC ingress server binds locally.
    pub fn grpc_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.grpc_port)
    }

    /// Address the Rakka TCP remoting transport binds locally.
    pub fn tcp_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.rakka_port)
    }
}

/// Returns the first set, non-empty value among the named environment variables.
fn first_env(names: &[&str]) -> Option<String> {
    names
        .iter()
        .filter_map(|name| env::var(name).ok())
        .find(|value| !value.trim().is_empty())
}
