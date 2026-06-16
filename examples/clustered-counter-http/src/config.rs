//! Environment-driven node configuration.

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use rakka::cluster::{ClusterNode, NodeAddress, NodeId};

use crate::support::{
    default_node_incarnation, env_u16, parse_u16, ExampleResult, DEFAULT_RAKKA_TCP_PORT,
};

#[derive(Debug, Clone)]
pub struct ExampleConfig {
    pub bind_host: IpAddr,
    pub advertise_host: String,
    pub tcp_port: u16,
    pub http_port: u16,
    pub node_logical_id: String,
    pub node_incarnation: String,
    pub discovery_dir: PathBuf,
    pub counter_store_dir: PathBuf,
}

impl ExampleConfig {
    pub fn from_env() -> ExampleResult<Self> {
        let tcp_port = env_u16("RAKKA_TCP_PORT", DEFAULT_RAKKA_TCP_PORT)?;
        let http_port = env::var("RAKKA_HTTP_PORT")
            .ok()
            .map(|value| parse_u16("RAKKA_HTTP_PORT", &value))
            .transpose()?
            .unwrap_or_else(|| tcp_port.saturating_add(10_000));
        let bind_host = env::var("RAKKA_BIND_HOST")
            .unwrap_or_else(|_| Ipv4Addr::LOCALHOST.to_string())
            .parse::<IpAddr>()?;
        let advertise_host =
            env::var("RAKKA_ADVERTISE_HOST").unwrap_or_else(|_| bind_host.to_string());

        // Logical ids are stable for a port, while incarnations change on
        // restart. That lets existing members distinguish a restarted process
        // from an old incarnation they already marked down.
        let node_logical_id = env::var("RAKKA_NODE_LOGICAL_ID")
            .unwrap_or_else(|_| format!("counter-node-{tcp_port}"));
        let node_incarnation = env::var("RAKKA_NODE_INCARNATION")
            .unwrap_or_else(|_| default_node_incarnation(tcp_port));

        let base_dir = env::temp_dir().join("rakka-clustered-counter-http");
        let discovery_dir = env::var_os("RAKKA_DISCOVERY_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| base_dir.join("discovery"));
        let counter_store_dir = env::var_os("RAKKA_COUNTER_STORE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| base_dir.join("counter-state"));

        Ok(Self {
            bind_host,
            advertise_host,
            tcp_port,
            http_port,
            node_logical_id,
            node_incarnation,
            discovery_dir,
            counter_store_dir,
        })
    }

    pub fn local_node(&self) -> ClusterNode {
        ClusterNode::new(
            NodeId::new(self.node_logical_id.clone(), self.node_incarnation.clone()),
            NodeAddress::new(self.advertise_host.clone(), self.tcp_port),
        )
        .with_role("counter")
    }

    pub fn tcp_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.tcp_port)
    }

    pub fn http_bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.bind_host, self.http_port)
    }
}
