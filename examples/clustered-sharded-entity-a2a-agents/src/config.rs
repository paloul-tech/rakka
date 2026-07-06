//! Environment-driven per-process configuration.

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use rakka::cluster::{ClusterNode, NodeAddress, NodeId};

use crate::support::{
    default_node_incarnation, default_node_logical_id, env_u16, env_u64, parse_u16, ExampleError,
    ExampleResult, DEFAULT_ETCD_LEASE_TTL_SECONDS, DEFAULT_ETCD_PREFIX, DEFAULT_RAKKA_PORT,
};

/// Which cluster-membership discovery source the process uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryProviderKind {
    /// Shared file directory for local development.
    File,
    /// etcd register/lease/discover for production-like testing.
    Etcd,
}

/// Which durable state backend the process uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceKind {
    /// File store for one-host local development.
    File,
    /// Shared PostgreSQL store for multi-pod recovery.
    Postgres,
}

/// Resolved configuration for one example process.
#[derive(Debug, Clone)]
pub struct ExampleConfig {
    /// Local bind host for HTTP and remoting.
    pub bind_host: IpAddr,
    /// Host advertised to other Rakka nodes for internal remoting.
    pub advertise_host: String,
    /// Rakka TCP remoting port.
    pub rakka_port: u16,
    /// Public HTTP server port.
    pub http_port: u16,
    /// Stable logical node id.
    pub node_logical_id: String,
    /// Per-process node incarnation id.
    pub node_incarnation: String,
    /// Discovery provider selected for this node.
    pub discovery_provider: DiscoveryProviderKind,
    /// Shared directory used by local file discovery.
    pub discovery_dir: PathBuf,
    /// etcd endpoints used when `discovery_provider` is `Etcd`.
    pub etcd_endpoints: Vec<String>,
    /// etcd key prefix used when `discovery_provider` is `Etcd`.
    pub etcd_prefix: String,
    /// etcd lease TTL in seconds.
    pub etcd_lease_ttl_seconds: i64,
    /// Durable state backend selected for this node.
    pub persistence: PersistenceKind,
    /// PostgreSQL DSN used when `persistence` is `Postgres`.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub postgres_dsn: Option<String>,
    /// Shared directory used by example file-backed durable stores.
    pub state_dir: PathBuf,
    /// Whether etcd mode self-fences after sustained peer unreachability.
    pub self_fence: bool,
    /// Sustained peer-unreachability before self-fencing.
    pub self_fence_after: Duration,
    /// Sustained peer-reachability before clearing a self-fence.
    pub self_fence_rejoin_after: Duration,
    /// Optional load-balanced public URL advertised in the agent card.
    pub public_url: Option<String>,
}

impl ExampleConfig {
    /// Builds configuration from environment variables, with local defaults.
    pub fn from_env() -> ExampleResult<Self> {
        let rakka_port = env_u16("RAKKA_PORT", DEFAULT_RAKKA_PORT)?;
        let http_port = env::var("RAKKA_HTTP_PORT")
            .ok()
            .map(|value| parse_u16("RAKKA_HTTP_PORT", &value))
            .transpose()?
            .unwrap_or_else(|| rakka_port.saturating_add(10_000));
        let bind_host = env::var("RAKKA_BIND_HOST")
            .unwrap_or_else(|_| Ipv4Addr::LOCALHOST.to_string())
            .parse::<IpAddr>()?;
        let advertise_host = first_env(&["RAKKA_ADVERTISE_HOST", "RAKKA_POD_IP"])
            .unwrap_or_else(|| bind_host.to_string());
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
                ))));
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
                ))));
            }
        };
        let postgres_dsn = env::var("RAKKA_POSTGRES_DSN").ok();
        let self_fence = env::var("RAKKA_SELF_FENCE")
            .map(|value| {
                !matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "0" | "off" | "false" | "no"
                )
            })
            .unwrap_or(true);
        let self_fence_after = Duration::from_secs(env_u64("RAKKA_SELF_FENCE_AFTER_SECONDS", 15));
        let self_fence_rejoin_after =
            Duration::from_secs(env_u64("RAKKA_SELF_FENCE_REJOIN_SECONDS", 10));
        let base_dir = env::temp_dir().join("rakka-clustered-sharded-entity-a2a-agents");
        let discovery_dir = env::var_os("RAKKA_DISCOVERY_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| base_dir.join("discovery"));
        let state_dir = env::var_os("RAKKA_STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| base_dir.join("state"));
        let public_url = env::var("RAKKA_A2A_PUBLIC_URL")
            .ok()
            .filter(|value| !value.trim().is_empty());

        Ok(Self {
            bind_host,
            advertise_host,
            rakka_port,
            http_port,
            node_logical_id,
            node_incarnation,
            discovery_provider,
            discovery_dir,
            etcd_endpoints,
            etcd_prefix,
            etcd_lease_ttl_seconds,
            persistence,
            postgres_dsn,
            state_dir,
            self_fence,
            self_fence_after,
            self_fence_rejoin_after,
            public_url,
        })
    }

    /// Stable cluster identity for this process.
    pub fn local_node(&self) -> ClusterNode {
        ClusterNode::new(
            NodeId::new(self.node_logical_id.clone(), self.node_incarnation.clone()),
            NodeAddress::new(self.advertise_host.clone(), self.rakka_port),
        )
        .with_role("a2a-agent")
    }

    /// Address the HTTP ingress server binds locally.
    pub fn http_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.http_port)
    }

    /// Address the Rakka TCP remoting transport binds locally.
    pub fn tcp_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.rakka_port)
    }

    /// Public base URL used by local developer-mode agent cards.
    ///
    /// Uses `advertise_host` rather than `bind_host` so a node bound to a
    /// wildcard address (`RAKKA_BIND_HOST=0.0.0.0`) still advertises a
    /// dialable per-node URL.
    pub fn local_public_url(&self) -> String {
        format!("http://{}:{}", self.advertise_host, self.http_port)
    }
}

fn first_env(names: &[&str]) -> Option<String> {
    names
        .iter()
        .filter_map(|name| env::var(name).ok())
        .find(|value| !value.trim().is_empty())
}
