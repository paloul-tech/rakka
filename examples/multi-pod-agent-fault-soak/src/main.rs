//! Multi-pod fault and soak validation for the durable agent domain.
//!
//! Run without arguments to drive the whole matrix:
//!
//! ```sh
//! cargo run -p rakka-example-multi-pod-agent-fault-soak
//! ```
//!
//! The binary re-execs *itself* as each pod, which is the idiom
//! `examples/multi-node-sharding` established for this repository's other
//! multi-process check. Each pod is a real OS process with its own actor
//! system, its own TCP transport, and no memory shared with any other; the
//! only thing they have in common is a directory.

use std::error::Error;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use rakka_agent::AgentTaskStatus;
use rakka_cluster::{ClusterNode, DiscoverySnapshot, NodeAddress, NodeId};
use rakka_example_multi_pod_agent_fault_soak::external::ledger_entries;
use rakka_example_multi_pod_agent_fault_soak::flow;
use rakka_example_multi_pod_agent_fault_soak::stores::PodCrash;
use rakka_example_multi_pod_agent_fault_soak::wiring::{boot_pod, CrashTarget, ROLE};
use tokio::process::Command;

/// How many drive rounds a pod runs before giving up on making progress.
const ROUNDS: usize = 4_000;

/// How long a pod drives before reporting what the durable record says.
const POD_DEADLINE: Duration = Duration::from_secs(20);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None | Some("--driver") => run_driver().await,
        Some("--node") => run_node(&args[1..]).await,
        other => Err(error(format!("unknown mode {other:?}; use --driver or --node")).into()),
    }
}

fn error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}

fn unused_port() -> std::io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// One pod process: boot, join, do its duty, exit.
async fn run_node(args: &[String]) -> Result<(), Box<dyn Error>> {
    let [logical, incarnation, root, port, peer_logical, peer_incarnation, peer_port, rest @ ..] =
        args
    else {
        return Err(error("--node takes seven positional arguments").into());
    };

    let crash = match rest {
        [] => None,
        [store, nth, window] => Some((
            CrashTarget::parse(store).ok_or_else(|| error("unknown crash store"))?,
            nth.parse::<usize>()?,
            PodCrash::parse(window).ok_or_else(|| error("unknown crash window"))?,
        )),
        _ => return Err(error("crash arming takes three arguments").into()),
    };

    let root = PathBuf::from(root);
    let Some(mut pod) = boot_pod(logical, incarnation, port.parse::<u16>()?, &root, crash).await
    else {
        // Loopback binding is unavailable; the driver treats this as a skip.
        println!("skip: loopback bind denied");
        return Ok(());
    };

    let peer = ClusterNode::new(
        NodeId::new(peer_logical.as_str(), peer_incarnation.as_str()),
        NodeAddress::new("127.0.0.1", peer_port.parse::<u16>()?),
    )
    .with_role(ROLE);
    pod.runtime.apply_discovery(DiscoverySnapshot::new(
        "multi-pod-agent-fault-soak",
        1,
        [pod.runtime.local_node().clone(), peer],
    ))?;

    // Both pods seed. The commands deduplicate on derived operation ids, so
    // this is one agent and one task however many pods issue them — and it is
    // what an ingress that redelivers to whichever pod is up actually does. A
    // seed that loses its compare-and-set to the other pod is not an error:
    // the record it wanted already exists.
    for _attempt in 0..5 {
        if flow::seed(&pod).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let terminal = flow::drive(&mut pod, ROUNDS, POD_DEADLINE)
        .await
        .map_err(error)?;

    println!(
        "pod {logical} done: terminal={terminal} task-writes={} run-writes={} \
         owns-task={} owns-run={} took-over={} status={:?}",
        pod.stores.tasks.writes(),
        pod.stores.runs.writes(),
        pod.owns_task(flow::task_scope().entity_id().as_str()),
        pod.owns_run(flow::run_scope().entity_id().as_str()),
        flow::TOOK_OVER.load(std::sync::atomic::Ordering::SeqCst),
        flow::task_status(&root).await,
    );
    pod.system.shutdown();
    Ok(())
}

/// Which pod an arming applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Armed {
    /// Kill `rakka-0`, which seeds and — in this shard layout — owns the run.
    PodA,
    /// Kill `rakka-1`, which owns the task.
    PodB,
}

/// One run of the two-pod world, optionally with one pod armed to die.
///
/// Both pods are ordinary background children. Whichever exits first has its
/// departure announced to the other, which downs it and takes over its shards.
/// That symmetry is the point: the pod that dies is chosen by the arming, not
/// by the harness's structure, so the task owner and the run owner are both
/// killable.
///
/// Returns the write counts each pod reported, when both survived to report.
async fn run_world(
    root: &Path,
    crash: Option<(Armed, CrashTarget, usize, PodCrash)>,
) -> Result<Option<PodWrites>, Box<dyn Error>> {
    let executable = std::env::current_exe()?;
    let port_a = unused_port()?;
    let port_b = unused_port()?;

    let node_args = |logical: &str,
                     incarnation: &str,
                     port: u16,
                     peer_logical: &str,
                     peer_incarnation: &str,
                     peer_port: u16,
                     armed: bool| {
        let mut args = vec![
            "--node".to_string(),
            logical.to_string(),
            incarnation.to_string(),
            root.display().to_string(),
            port.to_string(),
            peer_logical.to_string(),
            peer_incarnation.to_string(),
            peer_port.to_string(),
        ];
        if armed {
            if let Some((_, target, nth, window)) = crash {
                args.push(target.as_label().to_string());
                args.push(nth.to_string());
                args.push(window.as_label().to_string());
            }
        }
        args
    };

    let mut pod_a = Command::new(&executable)
        .args(node_args(
            "rakka-0",
            "uid-a",
            port_a,
            "rakka-1",
            "uid-b",
            port_b,
            crash.is_some_and(|(armed, ..)| armed == Armed::PodA),
        ))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut pod_b = Command::new(&executable)
        .args(node_args(
            "rakka-1",
            "uid-b",
            port_b,
            "rakka-0",
            "uid-a",
            port_a,
            crash.is_some_and(|(armed, ..)| armed == Armed::PodB),
        ))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    // Whichever pod leaves first — killed or finished — is announced to the
    // other. A survivor that guessed instead would be the second writer
    // specification 15 forbids.
    let departed = tokio::select! {
        status = pod_a.wait() => { status?; ("rakka-0", "uid-a") }
        status = pod_b.wait() => { status?; ("rakka-1", "uid-b") }
    };
    std::fs::write(
        root.join(flow::DEPARTED),
        format!("{} {}", departed.0, departed.1),
    )?;

    let wait_both = async {
        let _ = pod_a.wait().await;
        let _ = pod_b.wait().await;
    };
    if tokio::time::timeout(Duration::from_secs(60), wait_both)
        .await
        .is_err()
    {
        let _ = pod_a.kill().await;
        let _ = pod_b.kill().await;
        return Err(error("a pod outlived the harness deadline").into());
    }

    let out_a = drain(&mut pod_a).await;
    let out_b = drain(&mut pod_b).await;
    if std::env::var("RAKKA_MULTI_POD_VERBOSE").is_ok() {
        eprintln!("pod-a: {}pod-b: {}", out_a, out_b);
    }
    if out_a.contains("skip:") || out_b.contains("skip:") {
        return Ok(None);
    }
    match (parse_writes(&out_a), parse_writes(&out_b)) {
        (Some(a), Some(b)) => Ok(Some(PodWrites { pod_a: a, pod_b: b })),
        _ => Ok(None),
    }
}

/// What each pod reported writing, `(tasks, runs)` per pod.
#[derive(Debug, Clone, Copy)]
struct PodWrites {
    pod_a: (usize, usize),
    pod_b: (usize, usize),
}

async fn drain(child: &mut tokio::process::Child) -> String {
    use tokio::io::AsyncReadExt as _;

    let Some(mut out) = child.stdout.take() else {
        return String::new();
    };
    let mut buffer = String::new();
    let _ = out.read_to_string(&mut buffer).await;
    buffer
}

fn parse_writes(stdout: &str) -> Option<(usize, usize)> {
    let line = stdout.lines().find(|line| line.contains("task-writes="))?;
    let mut tasks = None;
    let mut runs = None;
    for field in line.split_whitespace() {
        if let Some(value) = field.strip_prefix("task-writes=") {
            tasks = value.parse().ok();
        }
        if let Some(value) = field.strip_prefix("run-writes=") {
            runs = value.parse().ok();
        }
    }
    Some((tasks?, runs?))
}

async fn assert_converged(root: &Path, context: &str) -> Result<(), Box<dyn Error>> {
    let status = flow::task_status(root).await;
    if status != Some(AgentTaskStatus::Completed) {
        return Err(error(format!(
            "{context}: the task did not converge on Completed, durable status is {status:?}"
        ))
        .into());
    }
    // The external system was reached, and reached for one logical turn. A
    // pod killed after the commit but before the receipt may legitimately
    // cause a *retry* of that same turn — what it may never cause is a second
    // logical turn under a different identity.
    let entries = ledger_entries(root);
    if entries.is_empty() {
        return Err(error(format!("{context}: the external system was never reached")).into());
    }
    let distinct: std::collections::BTreeSet<&str> = entries.iter().map(String::as_str).collect();
    if distinct.len() != 1 {
        return Err(error(format!(
            "{context}: the external system saw {} distinct calls, expected one logical turn: {distinct:?}",
            distinct.len()
        ))
        .into());
    }
    Ok(())
}

fn fresh_root(label: &str) -> std::io::Result<PathBuf> {
    let root = std::env::temp_dir().join(format!(
        "rakka-multi-pod-{}-{}-{label}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| value.as_nanos())
            .unwrap_or_default()
    ));
    std::fs::create_dir_all(&root)?;
    Ok(root)
}

async fn run_driver() -> Result<(), Box<dyn Error>> {
    println!("Rakka multi-pod agent fault harness");

    // The crash-free reference: two pods, a shared directory, and the task
    // driven to completion across them.
    let root = fresh_root("reference")?;
    let Some(writes) = run_world(&root, None).await? else {
        println!("skipped: loopback binding is unavailable in this environment");
        return Ok(());
    };
    assert_converged(&root, "the crash-free reference").await?;
    println!(
        "reference: two pods completed the task; pod-a wrote tasks={} runs={}, \
         pod-b wrote tasks={} runs={}",
        writes.pod_a.0, writes.pod_a.1, writes.pod_b.0, writes.pod_b.1
    );
    let _ = std::fs::remove_dir_all(&root);

    // Every durable write either pod makes is a window. The pod that owns the
    // task and the pod that owns the run are both killed, at both windows of
    // every write, and the survivor has to finish the work from the record.
    let plan = [
        (Armed::PodA, CrashTarget::Tasks, writes.pod_a.0),
        (Armed::PodA, CrashTarget::Runs, writes.pod_a.1),
        (Armed::PodB, CrashTarget::Tasks, writes.pod_b.0),
        (Armed::PodB, CrashTarget::Runs, writes.pod_b.1),
    ];
    if plan.iter().all(|(_, _, count)| *count == 0) {
        return Err(error("neither pod wrote anything; the sweep would prove nothing").into());
    }

    let mut windows = 0usize;
    for (armed, target, count) in plan {
        for nth in 1..=count {
            for window in [PodCrash::BeforeWrite, PodCrash::AfterWrite] {
                let label = format!(
                    "{armed:?}-{}-{nth}-{}",
                    target.as_label(),
                    window.as_label()
                );
                let root = fresh_root(&label)?;
                let _ = run_world(&root, Some((armed, target, nth, window))).await?;
                assert_converged(
                    &root,
                    &format!(
                        "{armed:?} killed at {} write {nth} ({})",
                        target.as_label(),
                        window.as_label()
                    ),
                )
                .await?;
                let _ = std::fs::remove_dir_all(&root);
                windows += 1;
            }
        }
    }

    println!("swept {windows} pod-loss windows; every one converged from the shared record");
    Ok(())
}
