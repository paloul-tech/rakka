//! Environment-driven per-process configuration.

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use rakka::cluster::{ClusterNode, NodeAddress, NodeId};

use crate::support::{
    default_node_incarnation, default_node_logical_id, env_u16, parse_u16, ExampleResult,
    DEFAULT_RAKKA_PORT,
};

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
    pub discovery_dir: PathBuf,
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
        let advertise_host =
            env::var("RAKKA_ADVERTISE_HOST").unwrap_or_else(|_| bind_host.to_string());

        // The logical id is stable for a port so a restarted process rejoins as
        // the same logical node; the incarnation changes every start.
        let node_logical_id = env::var("RAKKA_NODE_LOGICAL_ID")
            .unwrap_or_else(|_| default_node_logical_id(rakka_port));
        let node_incarnation = env::var("RAKKA_NODE_INCARNATION")
            .unwrap_or_else(|_| default_node_incarnation(rakka_port));

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
            discovery_dir,
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
