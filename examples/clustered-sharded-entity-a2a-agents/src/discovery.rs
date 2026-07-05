//! File-based local discovery for Phase 2 developer mode.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rakka::cluster::{ClusterNode, DiscoverySnapshot};
use rakka::sharding::ClusterNodeRuntime;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, Notify};

use crate::config::ExampleConfig;
use crate::support::{
    current_timestamp_millis, hex_encode, millis, ExampleResult, DEFAULT_DISCOVERY_POLL,
    DEFAULT_DISCOVERY_TTL,
};

/// Shared snapshot of up-member node ids.
pub type MembershipView = Arc<Mutex<Vec<String>>>;

/// Creates an empty membership view.
#[must_use]
pub fn new_membership_view() -> MembershipView {
    Arc::new(Mutex::new(Vec::new()))
}

/// Reads the current up-member node ids.
#[must_use]
pub fn membership_snapshot(view: &MembershipView) -> Vec<String> {
    view.lock().expect("membership view mutex").clone()
}

fn set_membership(view: &MembershipView, nodes: &[ClusterNode]) {
    let mut ids: Vec<String> = nodes.iter().map(|node| node.id().to_string()).collect();
    ids.sort();
    ids.dedup();
    *view.lock().expect("membership view mutex") = ids;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscoveryRecord {
    node: ClusterNode,
    updated_at_millis: u64,
}

/// Publishes and applies the initial file-discovery snapshot before serving.
pub fn seed_file_discovery(
    config: &ExampleConfig,
    local_node: &ClusterNode,
    runtime: &mut ClusterNodeRuntime,
    membership: &MembershipView,
) -> ExampleResult<()> {
    publish_discovery_record(&config.discovery_dir, local_node)?;
    let nodes = read_discovery_nodes(&config.discovery_dir, local_node)?;
    set_membership(membership, &nodes);
    runtime.apply_discovery(DiscoverySnapshot::new(
        "a2a-agent-file-discovery",
        current_timestamp_millis(),
        nodes,
    ))?;
    Ok(())
}

/// Runs the file-discovery loop until `shutdown` is signalled.
pub async fn run_file_discovery(
    runtime: Arc<AsyncMutex<ClusterNodeRuntime>>,
    config: ExampleConfig,
    local_node: ClusterNode,
    membership: MembershipView,
    shutdown: Arc<Notify>,
) {
    let mut interval = tokio::time::interval(DEFAULT_DISCOVERY_POLL);
    loop {
        tokio::select! {
            () = shutdown.notified() => {
                let _ = remove_discovery_record(&config.discovery_dir, local_node.id().logical_id());
                break;
            }
            _ = interval.tick() => {
                if let Err(error) = publish_discovery_record(&config.discovery_dir, &local_node) {
                    eprintln!("discovery publish failed: {error}");
                    continue;
                }
                let nodes = match read_discovery_nodes(&config.discovery_dir, &local_node) {
                    Ok(nodes) => nodes,
                    Err(error) => {
                        eprintln!("discovery read failed: {error}");
                        continue;
                    }
                };
                let now = current_timestamp_millis();
                set_membership(&membership, &nodes);
                let mut runtime = runtime.lock().await;
                if let Err(error) = runtime.apply_discovery(DiscoverySnapshot::new(
                    "a2a-agent-file-discovery",
                    now,
                    nodes,
                )) {
                    eprintln!("discovery apply failed: {error}");
                }
                if let Err(error) = runtime.tick(now) {
                    eprintln!("membership tick failed: {error}");
                }
            }
        }
    }
}

fn publish_discovery_record(dir: &Path, node: &ClusterNode) -> ExampleResult<()> {
    std::fs::create_dir_all(dir)?;
    let path = discovery_record_path(dir, node.id().logical_id());
    let temp = path.with_extension(format!("json.tmp.{}", current_timestamp_millis()));
    let record = DiscoveryRecord {
        node: node.clone(),
        updated_at_millis: current_timestamp_millis(),
    };
    let bytes = serde_json::to_vec_pretty(&record)?;
    std::fs::write(&temp, bytes)?;
    std::fs::rename(temp, path)?;
    Ok(())
}

fn read_discovery_nodes(dir: &Path, local_node: &ClusterNode) -> ExampleResult<Vec<ClusterNode>> {
    let now = current_timestamp_millis();
    let ttl = millis(DEFAULT_DISCOVERY_TTL);
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(vec![local_node.clone()]);
        }
        Err(error) => return Err(error.into()),
    };
    let mut nodes = Vec::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let bytes = std::fs::read(&path)?;
        let record: DiscoveryRecord = serde_json::from_slice(&bytes)?;
        if now.saturating_sub(record.updated_at_millis) <= ttl
            || record.node.id() == local_node.id()
        {
            nodes.push(record.node);
        }
    }
    if !nodes.iter().any(|node| node.id() == local_node.id()) {
        nodes.push(local_node.clone());
    }
    Ok(nodes)
}

fn remove_discovery_record(dir: &Path, logical_id: &str) -> ExampleResult<()> {
    match std::fs::remove_file(discovery_record_path(dir, logical_id)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn discovery_record_path(dir: &Path, logical_id: &str) -> PathBuf {
    dir.join(format!("{}.json", hex_encode(logical_id)))
}
