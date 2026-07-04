//! Environment-driven per-process configuration.

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use rakka::cluster::{ClusterNode, NodeAddress, NodeId};

use crate::support::{
    default_node_incarnation, default_node_logical_id, env_u16, parse_u16, ExampleResult,
    DEFAULT_RAKKA_PORT,
};

/// Resolved configuration for one Phase 0 example process.
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
    /// Shared directory used by local file discovery.
    pub discovery_dir: PathBuf,
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
        let base_dir = env::temp_dir().join("rakka-clustered-sharded-entity-a2a-agents");
        let discovery_dir = env::var_os("RAKKA_DISCOVERY_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| base_dir.join("discovery"));
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
            discovery_dir,
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
