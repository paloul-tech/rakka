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
use rakka_example_multi_pod_agent_fault_soak::stores::{PodCrash, CRASHED};
use rakka_example_multi_pod_agent_fault_soak::wiring::{boot_pod, CrashTarget, ROLE};
use tokio::process::Command;

/// How many drive rounds a pod runs before giving up on making progress.
const ROUNDS: usize = 4_000;

/// How long a pod drives before reporting what the durable record says.
const POD_DEADLINE: Duration = Duration::from_secs(20);

/// The highest write ordinal a sweep row will arm before giving up on finding
/// the flow's last write. A row that stops here says so rather than reporting
/// a bounded sweep as an exhaustive one.
const SWEEP_CEILING: usize = 64;

/// The fewest windows the second owner's own *run-store* writes must fire.
///
/// Measured at 20 — the second owner redrives the whole run from the record, so
/// it writes about as often as the first owner does. Well under that, and far
/// over a two-arming world that has stopped reaching the inherited shard.
const TAKEOVER_RUNS_FLOOR: usize = 12;

/// The fewest windows the second owner's own *outbox* writes must fire.
///
/// A thin row, like the first owner's outbox row: the effects of one redriven
/// run are few. Thin is not absent — this is the effect boundary specification
/// 18's directive names, reached by the pod that inherited it.
const TAKEOVER_WORKFLOW_FLOOR: usize = 1;

/// The fewest windows that must move a shard to the surviving pod.
///
/// Not every window can. An armed pod that dies after finishing its own part
/// leaves a survivor that completes from the shared record without ever needing
/// the dead pod's shards — a real recovery, but not a takeover. Measured at 25
/// of 33 windows, so this floor sits well under what the sweep reaches and far
/// above a sweep that has stopped downing peers altogether.
const TAKEOVER_FLOOR: usize = 8;

/// How many consecutive ordinals must fire nothing before a row is spent.
///
/// One is not enough. The write counters move with TCP timing, membership
/// convergence, and which pod wins the seed compare-and-set, so an armed pod
/// can miss ordinal `n` in the two worlds that arm it and still reach `n + 1`
/// in the two that arm that. Stopping at the first miss silently truncates the
/// row and reports the remainder as swept; walking past it turns the miss into
/// a gap the row reports.
const DRY_ORDINALS_TO_STOP: usize = 2;

/// The sweep, and what each row is expected to reach.
///
/// The floor is what makes a collapsed row fail instead of reading as a short
/// one: `PodB runs` is legitimately zero, so a bare "did anything fire?" guard
/// cannot tell an intended zero from a regression that stopped a row dead. Each
/// floor sits well under what the flow actually does — the rows measured 6, 19
/// and 6 windows — and well over nothing.
const SWEEP_ROWS: [SweepRow; 8] = [
    SweepRow::reachable(Armed::PodA, CrashTarget::Tasks, 4),
    SweepRow::reachable(Armed::PodA, CrashTarget::Runs, 12),
    SweepRow::reachable(Armed::PodB, CrashTarget::Tasks, 4),
    // Thin rows: each of these stores takes exactly one write from the pod
    // that reaches it, so the floor is one window rather than a whole ordinal's
    // pair. Thin is not the same as absent — the workflow outbox is the effect
    // boundary specification 18's directive names, and it could not be armed at
    // all until now.
    SweepRow::reachable(Armed::PodA, CrashTarget::Agents, 1),
    SweepRow::reachable(Armed::PodA, CrashTarget::Workflow, 1),
    SweepRow::unreachable(
        Armed::PodB,
        CrashTarget::Agents,
        "pod A seeds 150ms earlier, so pod B's identical instantiation deduplicates on the same derived operation id and commits nothing",
    ),
    SweepRow::unreachable(
        Armed::PodB,
        CrashTarget::Workflow,
        "pod B emits no effects until it owns the run; the second-owner rows sweep them",
    ),
    // Unreachable *by a single arming*, and printed rather than hidden: pod B
    // drives the run only once pod A is gone, and pod A goes only when it is
    // the armed pod. The second-owner row below builds the world with two
    // armings that does reach them.
    SweepRow::unreachable(
        Armed::PodB,
        CrashTarget::Runs,
        "pod B reaches its run writes only after taking over; the second-owner row sweeps them",
    ),
];

/// One row of the sweep.
struct SweepRow {
    /// The pod this row arms.
    armed: Armed,
    /// The durable class this row arms it inside.
    target: CrashTarget,
    /// The fewest windows the row must fire, or `None` with the reason when
    /// the harness structurally cannot reach the row at all.
    floor: Option<usize>,
    /// Why an unreachable row is unreachable.
    unreachable_because: Option<&'static str>,
}

impl SweepRow {
    const fn reachable(armed: Armed, target: CrashTarget, floor: usize) -> Self {
        Self {
            armed,
            target,
            floor: Some(floor),
            unreachable_because: None,
        }
    }

    const fn unreachable(armed: Armed, target: CrashTarget, because: &'static str) -> Self {
        Self {
            armed,
            target,
            floor: None,
            unreachable_because: Some(because),
        }
    }
}

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

/// Two distinct free loopback ports.
///
/// Not `rakka_process::testkit::available_tcp_port`: it returns one port and
/// drops its listener before the next call, which is the collision this exists
/// to avoid.
///
/// Both listeners are held at once and dropped together, so the kernel cannot
/// hand the same port to both pods — which binding and dropping one at a time
/// allowed, and which made pod B fail to bind. The window in which another
/// process can take one of them is not closable this way (the transport does
/// its own bind), so a pod that finds its port taken says so and the driver
/// retries the world with fresh ports rather than reporting a skip.
fn unused_ports(count: usize) -> std::io::Result<Vec<u16>> {
    let listeners = (0..count)
        .map(|_| TcpListener::bind(("127.0.0.1", 0)))
        .collect::<std::io::Result<Vec<_>>>()?;
    listeners
        .iter()
        .map(|listener| Ok(listener.local_addr()?.port()))
        .collect()
}

/// Whether this environment permits loopback binding at all.
///
/// Settled once, before any world runs. It is the only thing that may turn the
/// whole harness into a skip: every other failure now keeps its message and
/// fails. Previously eight unrelated wiring failures inside `boot_pod` were all
/// reported as this, so the gated harness could exit 0 having proved nothing.
fn loopback_available() -> bool {
    TcpListener::bind(("127.0.0.1", 0)).is_ok()
}

/// One pod process: boot, join, do its duty, exit.
async fn run_node(args: &[String]) -> Result<(), Box<dyn Error>> {
    // `peers` is a comma-separated `logical:incarnation:port` list rather than
    // one fixed peer, because a world that replaces a lost pod has three.
    let [logical, incarnation, root, port, peers, prove, rest @ ..] = args else {
        return Err(error("--node takes six positional arguments").into());
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
    let port = port.parse::<u16>()?;

    // The driver settled that loopback binding works before spawning anything,
    // so a port that will not bind here is one another process took since. The
    // driver retries the world with fresh ports; every other failure below is
    // real and is returned with its message.
    if TcpListener::bind(("127.0.0.1", port)).is_err() {
        println!("port-taken: {port}");
        return Ok(());
    }
    let mut pod = boot_pod(logical, incarnation, port, &root, crash)
        .await
        .map_err(error)?;

    let mut members = vec![pod.runtime.local_node().clone()];
    for peer in peers.split(',').filter(|peer| !peer.is_empty()) {
        let mut parts = peer.split(':');
        let (Some(peer_logical), Some(peer_incarnation), Some(peer_port)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return Err(error(format!("malformed peer {peer:?}")).into());
        };
        members.push(
            ClusterNode::new(
                NodeId::new(peer_logical, peer_incarnation),
                NodeAddress::new("127.0.0.1", peer_port.parse::<u16>()?),
            )
            .with_role(ROLE),
        );
    }
    pod.runtime.apply_discovery(DiscoverySnapshot::new(
        "multi-pod-agent-fault-soak",
        1,
        members,
    ))?;

    // Both pods seed. The commands deduplicate on derived operation ids, so
    // this is one agent and one task however many pods issue them — and it is
    // what an ingress that redelivers to whichever pod is up actually does. A
    // seed that loses its compare-and-set to the other pod is not an error:
    // the record it wanted already exists.
    // Wider than the driver's own 150 ms spawn stagger, which the previous
    // budget of five 20 ms attempts was not. A seed that loses its
    // compare-and-set to the other pod is not an error — the record it wanted
    // already exists — so this is not fatal; what it must not be is silent.
    let mut seed_error = None;
    for _attempt in 0..20 {
        match flow::seed(&pod).await {
            Ok(()) => {
                seed_error = None;
                break;
            }
            Err(error) => seed_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if let Some(error) = &seed_error {
        eprintln!("pod {logical} never seeded: {error}");
    }
    // Before driving: replay the instantiation through the agent entity's
    // sharded command surface, which is the only path that exercises the
    // agent class's remote registration and its payload codecs.
    let agent_command = if prove == "prove" {
        flow::prove_remote_agent_command(&pod).await
    } else {
        "skipped"
    };

    let outcome = flow::drive(&mut pod, ROUNDS, POD_DEADLINE)
        .await
        .map_err(error)?;
    let terminal = outcome.terminal;

    // On its own line, so the done line stays whitespace-parseable and the
    // driver can echo the cause of a world that failed to converge instead of
    // reporting only that it did.
    if let Some(last_error) = &outcome.last_error {
        println!("pod {logical} drive-error: {last_error}");
    }

    println!(
        "pod {logical} done: terminal={terminal} task-writes={} run-writes={} \
         owns-task={} owns-run={} took-over={} agent-command={agent_command} \
         lost-writes={} status={:?}",
        pod.stores.tasks.writes(),
        pod.stores.runs.writes(),
        pod.owns_task(flow::task_scope().entity_id().as_str()),
        pod.owns_run(
            flow::run_scope(&pod.stores.tasks)
                .await
                .entity_id()
                .as_str(),
        ),
        outcome.took_over,
        outcome.lost_writes,
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
/// Runs one world, re-running it on fresh ports if a pod's port was taken.
async fn run_world(
    root: &Path,
    crash: Option<(Armed, CrashTarget, usize, PodCrash)>,
) -> Result<World, Box<dyn Error>> {
    for _attempt in 0..WORLD_BIND_ATTEMPTS {
        match run_world_once(root, crash).await? {
            // A pod that never bound may still have left a partly-seeded
            // directory behind, and the retry has to start from nothing.
            World::PortTaken => {
                let _ = std::fs::remove_dir_all(root);
                std::fs::create_dir_all(root)?;
            }
            outcome => return Ok(outcome),
        }
    }
    Err(error(format!(
        "a pod found its port taken on all {WORLD_BIND_ATTEMPTS} attempts"
    ))
    .into())
}

/// Returns what the world did, which is what bounds the sweep.
async fn run_world_once(
    root: &Path,
    crash: Option<(Armed, CrashTarget, usize, PodCrash)>,
) -> Result<World, Box<dyn Error>> {
    let executable = std::env::current_exe()?;
    let ports = unused_ports(2)?;
    let (port_a, port_b) = (ports[0], ports[1]);

    let node_args = |logical: &str, incarnation: &str, port: u16, peers: &str, armed: bool| {
        // Only the crash-free reference world proves the agent entity's remote
        // command arm. Every armed world has a pod that dies, and asking a dead
        // peer costs an ask timeout per world for a property the reference has
        // already established for this binary.
        let prove = if crash.is_none() { "prove" } else { "no-prove" };
        let mut args = vec![
            "--node".to_string(),
            logical.to_string(),
            incarnation.to_string(),
            root.display().to_string(),
            port.to_string(),
            peers.to_string(),
            prove.to_string(),
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
            &format!("rakka-1:uid-b:{port_b}"),
            crash.is_some_and(|(armed, ..)| armed == Armed::PodA),
        ))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        // Tokio does not kill on drop by default, so every early `?` below used
        // to return with both pods still running — to their own 20s deadline,
        // in a directory the driver had stopped watching and never cleaned up.
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut pod_b = Command::new(&executable)
        .args(node_args(
            "rakka-1",
            "uid-b",
            port_b,
            &format!("rakka-0:uid-a:{port_a}"),
            crash.is_some_and(|(armed, ..)| armed == Armed::PodB),
        ))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;

    // Whichever pod leaves first — killed or finished — is announced to the
    // other. A survivor that guessed instead would be the second writer
    // specification 15 forbids.
    //
    // Bounded, like the wait below it. This select had no deadline and no kill
    // path, so a pod wedged in an ask against a peer that bound but never
    // answered, or blocked on a full 64KiB stdout pipe nothing drains until
    // both waits return, hung the driver forever — and the gate tests shell out
    // with no timeout of their own.
    let mut status_a = None;
    let mut status_b = None;
    let departed = match tokio::time::timeout(WORLD_DEADLINE, async {
        tokio::select! {
            status = pod_a.wait() => (status, true),
            status = pod_b.wait() => (status, false),
        }
    })
    .await
    {
        Ok((status, true)) => {
            status_a = Some(status?);
            ("rakka-0", "uid-a")
        }
        Ok((status, false)) => {
            status_b = Some(status?);
            ("rakka-1", "uid-b")
        }
        Err(_) => return Err(error("neither pod exited before the harness deadline").into()),
    };
    flow::announce_departure(root, departed.0, departed.1)?;

    let remaining = tokio::time::timeout(WORLD_DEADLINE, async {
        if status_a.is_none() {
            status_a = Some(pod_a.wait().await?);
        }
        if status_b.is_none() {
            status_b = Some(pod_b.wait().await?);
        }
        Ok::<(), std::io::Error>(())
    })
    .await;
    match remaining {
        Ok(Ok(())) => {}
        Ok(Err(io)) => return Err(io.into()),
        Err(_) => return Err(error("a pod outlived the harness deadline").into()),
    }

    let out_a = drain(&mut pod_a).await;
    let out_b = drain(&mut pod_b).await;
    if std::env::var("RAKKA_MULTI_POD_VERBOSE").is_ok() {
        eprintln!("pod-a: {}pod-b: {}", out_a, out_b);
    }
    // Always, not only under the verbose flag: a drive loop that errored every
    // round is the diagnosis for a world that does not converge, and it used to
    // be discarded entirely.
    for line in out_a.lines().chain(out_b.lines()) {
        if line.contains("drive-error:") {
            eprintln!("{line}");
        }
    }
    if out_a.contains("port-taken:") || out_b.contains("port-taken:") {
        return Ok(World::PortTaken);
    }
    if root.join(CRASHED).exists() {
        // Which pod survived is the arming, not whichever one happens to have
        // reported. An armed pod can reach its write inside `system.shutdown()`
        // — after its work is done and its line already flushed — so "both pods
        // reported" is a real and convergent outcome, not a contradiction.
        let (report, status, who) = match crash.map(|(armed, ..)| armed) {
            Some(Armed::PodA) => (parse_report(&out_b), status_b, "pod-b"),
            Some(Armed::PodB) => (parse_report(&out_a), status_a, "pod-a"),
            None => {
                return Err(error("a crash marker was written in a world with no arming").into())
            }
        };
        let Some(survivor) = report else {
            return Err(error(format!(
                "a crash marker was written and the surviving {who} never reported: \
                 it died too"
            ))
            .into());
        };
        require_success(status, &format!("the surviving {who}"))?;
        return Ok(World::Crashed(survivor));
    }
    match (parse_report(&out_a), parse_report(&out_b)) {
        (Some(pod_a), Some(pod_b)) => {
            require_success(status_a, "pod-a")?;
            require_success(status_b, "pod-b")?;
            Ok(World::Survived(pod_a, pod_b))
        }
        // No skip, no crash marker, and a pod that never reported. Something
        // died that the harness did not arm, and reading that as a skip or as
        // a spent sweep row is how a real failure disappears into a green run.
        _ => Err(error(format!(
            "a pod exited without reporting its writes and without an armed crash\n\
             pod-a: {out_a}pod-b: {out_b}"
        ))
        .into()),
    }
}

/// What the two-arming world did.
#[derive(Debug)]
enum Takeover {
    /// The second owner reached its armed write and died, and the replacement
    /// finished from the shared record.
    Crashed,
    /// The second owner never reached the armed write.
    Survived,
    /// A pod found its port taken.
    PortTaken,
}

/// The world that sweeps the *second* owner's own durable writes.
///
/// Every other row kills the pod that natively owns what it is writing. This
/// one kills the pod that **inherited** it, which no single-arming world can
/// reach: pod B drives the run only after taking it over, and pod A gives it up
/// only by dying — so arming pod B alone finds it with nothing to write, which
/// is the `0 windows` row the sweep used to print and document as a limit.
///
/// Two armings and three pods. Pod A dies at its first task-store write, so it
/// never drives anything and pod B must take the run's shard over. Pod B is
/// armed at its `nth` run-store write, and because it makes no run-store writes
/// until it owns that shard, every ordinal here is a write it made as the
/// second owner — recovering from a record it did not create. Pod C replaces
/// it, downs both departed pods, and finishes.
///
/// Pods A and B see only each other, so while they run the cluster is the same
/// two-node shape every other row sweeps; pod C joins knowing all three, which
/// is what lets it down them.
async fn run_takeover_world(
    root: &Path,
    target: CrashTarget,
    nth: usize,
    window: PodCrash,
) -> Result<Takeover, Box<dyn Error>> {
    for _attempt in 0..WORLD_BIND_ATTEMPTS {
        match run_takeover_world_once(root, target, nth, window).await? {
            Takeover::PortTaken => {
                let _ = std::fs::remove_dir_all(root);
                std::fs::create_dir_all(root)?;
            }
            outcome => return Ok(outcome),
        }
    }
    Err(error(format!(
        "a pod found its port taken on all {WORLD_BIND_ATTEMPTS} attempts"
    ))
    .into())
}

async fn run_takeover_world_once(
    root: &Path,
    target: CrashTarget,
    nth: usize,
    window: PodCrash,
) -> Result<Takeover, Box<dyn Error>> {
    let executable = std::env::current_exe()?;
    let ports = unused_ports(3)?;
    let (port_a, port_b, port_c) = (ports[0], ports[1], ports[2]);

    let spawn = |logical: &str,
                 incarnation: &str,
                 port: u16,
                 peers: String,
                 crash: Option<(CrashTarget, usize, PodCrash)>| {
        let mut args = vec![
            "--node".to_string(),
            logical.to_string(),
            incarnation.to_string(),
            root.display().to_string(),
            port.to_string(),
            peers,
            "no-prove".to_string(),
        ];
        if let Some((target, nth, window)) = crash {
            args.push(target.as_label().to_string());
            args.push(nth.to_string());
            args.push(window.as_label().to_string());
        }
        Command::new(&executable)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
    };

    let mut pod_a = spawn(
        "rakka-0",
        "uid-a",
        port_a,
        format!("rakka-1:uid-b:{port_b}"),
        Some((CrashTarget::Tasks, 1, PodCrash::BeforeWrite)),
    )?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut pod_b = spawn(
        "rakka-1",
        "uid-b",
        port_b,
        format!("rakka-0:uid-a:{port_a}"),
        Some((target, nth, window)),
    )?;

    let status_a = match tokio::time::timeout(WORLD_DEADLINE, pod_a.wait()).await {
        Ok(status) => status?,
        Err(_) => return Err(error("the first pod outlived the harness deadline").into()),
    };
    flow::announce_departure(root, "rakka-0", "uid-a")?;

    let status_b = match tokio::time::timeout(WORLD_DEADLINE, pod_b.wait()).await {
        Ok(status) => status?,
        Err(_) => return Err(error("the second pod outlived the harness deadline").into()),
    };

    let out_a = drain(&mut pod_a).await;
    let out_b = drain(&mut pod_b).await;
    if out_a.contains("port-taken:") || out_b.contains("port-taken:") {
        return Ok(Takeover::PortTaken);
    }
    for line in out_a.lines().chain(out_b.lines()) {
        if line.contains("drive-error:") {
            eprintln!("{line}");
        }
    }
    if parse_report(&out_a).is_some() {
        return Err(error(
            "the first pod was armed at its first task-store write and did not die, so the \
             second pod never inherited anything",
        )
        .into());
    }
    let _ = status_a;

    // Pod B reported, so it outlived its armed write: the ordinal is past what
    // the second owner actually writes. No replacement is needed, and the world
    // still has to converge — pod A died in it.
    if let Some(report) = parse_report(&out_b) {
        require_success(Some(status_b), "the second owner")?;
        if !report.took_over {
            return Err(error(
                "the second pod finished without taking over, so its writes were not the \
                 second owner's",
            )
            .into());
        }
        return Ok(Takeover::Survived);
    }

    flow::announce_departure(root, "rakka-1", "uid-b")?;
    let mut pod_c = spawn(
        "rakka-2",
        "uid-c",
        port_c,
        format!("rakka-0:uid-a:{port_a},rakka-1:uid-b:{port_b}"),
        None,
    )?;
    let status_c = match tokio::time::timeout(WORLD_DEADLINE, pod_c.wait()).await {
        Ok(status) => status?,
        Err(_) => return Err(error("the replacement pod outlived the harness deadline").into()),
    };
    let out_c = drain(&mut pod_c).await;
    if out_c.contains("port-taken:") {
        return Ok(Takeover::PortTaken);
    }
    for line in out_c.lines() {
        if line.contains("drive-error:") {
            eprintln!("{line}");
        }
    }
    require_success(Some(status_c), "the replacement pod-c")?;
    if parse_report(&out_c).is_none() {
        return Err(error("the replacement pod never reported its writes").into());
    }
    Ok(Takeover::Crashed)
}

/// Sweeps the second owner's own writes to one store.
///
/// The loop is the same shape as the single-arming rows — walk ordinals until
/// two consecutive ones fire nothing, hold the row to a floor — over the
/// two-arming world instead. `run_takeover_world` arms the *second* owner in
/// `target`, and the second owner touches no store until it inherits the run's
/// shard, so every ordinal here is a write it made as the inheritor.
async fn sweep_second_owner(target: CrashTarget, floor: usize) -> Result<usize, Box<dyn Error>> {
    let mut fired = 0usize;
    let mut dry_streak = 0usize;
    let mut last_firing = 0usize;
    let mut nth = 1usize;
    while nth <= SWEEP_CEILING && dry_streak < DRY_ORDINALS_TO_STOP {
        let mut reached = false;
        for window in [PodCrash::BeforeWrite, PodCrash::AfterWrite] {
            let label = format!("takeover-{}-{nth}-{}", target.as_label(), window.as_label());
            let context = format!(
                "the second owner killed at {} write {nth} ({})",
                target.as_label(),
                window.as_label()
            );
            let root = fresh_root(&label)?;
            // `OneIdentity` in both arms: the first pod dies in this world
            // either way, so a retry of the same turn is legitimate.
            match run_takeover_world(&root, target, nth, window).await? {
                Takeover::Crashed => {
                    assert_converged(&root, &context, Ledger::OneIdentity).await?;
                    reached = true;
                    fired += 1;
                }
                Takeover::Survived => {
                    assert_converged(
                        &root,
                        &format!("{context}, which never fired"),
                        Ledger::OneIdentity,
                    )
                    .await?;
                }
                Takeover::PortTaken => {
                    unreachable!("run_takeover_world retries or fails on a taken port")
                }
            }
            let _ = std::fs::remove_dir_all(&root);
        }
        if reached {
            last_firing = nth;
            dry_streak = 0;
        } else {
            dry_streak += 1;
        }
        nth += 1;
    }
    println!(
        "  second owner {}: {fired} windows, ordinals 1-{last_firing}",
        target.as_label()
    );
    if fired < floor {
        return Err(error(format!(
            "the second owner's {} writes fired {fired} windows, below the floor of {floor}: \
             the two-arming world stopped reaching them",
            target.as_label()
        ))
        .into());
    }
    Ok(fired)
}

/// A pod whose work is evidence must have exited cleanly.
///
/// The survivor performs the recovery a window exists to prove, so one that
/// panicked, returned an error, or was killed leaves a converged record nothing
/// produced on purpose. Only the armed pod may exit abnormally, and it is the
/// one that leaves no report to read. Neither status was inspected at all
/// before: `status?` kept only the `io::Result` of waiting, and the second wait
/// discarded both.
fn require_success(
    status: Option<std::process::ExitStatus>,
    who: &str,
) -> Result<(), Box<dyn Error>> {
    match status {
        Some(status) if status.success() => Ok(()),
        Some(status) => Err(error(format!(
            "{who} exited with {status}, so what it did is not evidence of anything"
        ))
        .into()),
        None => Err(error(format!("{who} never reported an exit status")).into()),
    }
}

/// What one world did.
#[derive(Debug)]
enum World {
    /// Both pods ran to completion and reported their writes: either nothing
    /// was armed, or the armed write is past the end of what this pod's flow
    /// actually does. Carries pod A's report and then pod B's.
    Survived(PodReport, PodReport),
    /// The armed pod reached its armed write and aborted inside the window,
    /// and the surviving pod reported what it owned and whether it took over.
    Crashed(PodReport),
    /// A pod found its port taken between the driver choosing it and the pod
    /// binding it. Not a skip and not a failure — the world is re-run on fresh
    /// ports.
    PortTaken,
}

/// How many times a world is re-run when a pod finds its port taken.
const WORLD_BIND_ATTEMPTS: usize = 4;

/// How long the driver waits on either pod before giving up on the world.
///
/// Comfortably over `POD_DEADLINE`, which a pod restarts when it takes over its
/// peer's shards, so a world that reaches this has stopped making progress
/// rather than merely taken the long path.
const WORLD_DEADLINE: Duration = Duration::from_secs(90);

/// What one pod reported about the agent entity's sharded command surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgentCommandArm {
    /// This pod owns the agent entity; the command was a local ask.
    Local,
    /// This pod does not own it; the command crossed the wire and came back,
    /// through both payload codecs.
    Remote,
    /// The command did not complete. A failure wherever it was asked for.
    Failed,
    /// This world did not ask for the proof; only the reference world does.
    Skipped,
}

/// What one pod reported when it stopped.
///
/// The pods have always printed which entities they owned and whether they
/// downed a departed peer; the driver used to read only the write counts, so a
/// window counted as a proven pod-loss recovery even when nothing moved.
#[derive(Debug, Clone, Copy)]
struct PodReport {
    /// Durable writes this pod made to the task store.
    task_writes: usize,
    /// Durable writes this pod made to the run store.
    run_writes: usize,
    /// Whether this pod owned the task's shard when it stopped.
    owns_task: bool,
    /// Whether this pod owned the run's shard when it stopped.
    owns_run: bool,
    /// Whether this pod downed a departed peer and took over its shards.
    took_over: bool,
    /// Whether this pod saw the task reach a terminal status.
    terminal: bool,
    /// Which arm of the agent entity's sharded command surface this pod took.
    agent_command: AgentCommandArm,
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

fn parse_report(stdout: &str) -> Option<PodReport> {
    let line = stdout.lines().find(|line| line.contains("task-writes="))?;
    let mut task_writes = None;
    let mut run_writes = None;
    let mut owns_task = None;
    let mut owns_run = None;
    let mut took_over = None;
    let mut terminal = None;
    let mut agent_command = None;
    for field in line.split_whitespace() {
        if let Some(value) = field.strip_prefix("task-writes=") {
            task_writes = value.parse().ok();
        }
        if let Some(value) = field.strip_prefix("run-writes=") {
            run_writes = value.parse().ok();
        }
        if let Some(value) = field.strip_prefix("owns-task=") {
            owns_task = value.parse().ok();
        }
        if let Some(value) = field.strip_prefix("owns-run=") {
            owns_run = value.parse().ok();
        }
        if let Some(value) = field.strip_prefix("took-over=") {
            took_over = value.parse().ok();
        }
        if let Some(value) = field.strip_prefix("terminal=") {
            terminal = value.parse().ok();
        }
        if let Some(value) = field.strip_prefix("agent-command=") {
            agent_command = match value {
                "local" => Some(AgentCommandArm::Local),
                "remote" => Some(AgentCommandArm::Remote),
                "failed" => Some(AgentCommandArm::Failed),
                "skipped" => Some(AgentCommandArm::Skipped),
                _ => None,
            };
        }
    }
    Some(PodReport {
        task_writes: task_writes?,
        run_writes: run_writes?,
        owns_task: owns_task?,
        owns_run: owns_run?,
        took_over: took_over?,
        terminal: terminal?,
        agent_command: agent_command?,
    })
}

/// What the external ledger must show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ledger {
    /// Exactly one call. A world with no crash has nothing to retry through,
    /// so the exact count is known and free — and counting is the only thing
    /// that catches a turn re-issued on every round, which the identity check
    /// below cannot see because every line is identical.
    ExactlyOnce,
    /// One identity, however many times it was retried. A pod killed after the
    /// external commit but before the receipt legitimately causes a retry of
    /// the same turn under the same run; what it may never cause is a call
    /// under a different one.
    OneIdentity,
}

async fn assert_converged(
    root: &Path,
    context: &str,
    ledger: Ledger,
) -> Result<(), Box<dyn Error>> {
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
    if ledger == Ledger::ExactlyOnce && entries.len() != 1 {
        return Err(error(format!(
            "{context}: the external system was called {} times in a world with no crash to \
             retry through, expected exactly one: {entries:?}",
            entries.len()
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

    // The only skip in the harness. Everything after this point that fails,
    // fails loudly: a codec collision, a duplicate entity key, or a renamed
    // entity type used to arrive here as this same message and an exit code of
    // zero, which both gate tests accepted as a pass.
    if !loopback_available() {
        println!("skipped: loopback binding is unavailable in this environment");
        return Ok(());
    }

    // The crash-free reference: two pods, a shared directory, and the task
    // driven to completion across them.
    let root = fresh_root("reference")?;
    let (pod_a, pod_b) = match run_world(&root, None).await? {
        World::Survived(pod_a, pod_b) => (pod_a, pod_b),
        World::Crashed(_) => {
            return Err(error("the crash-free reference world reported a crash").into())
        }
        World::PortTaken => unreachable!("run_world retries or fails on a taken port"),
    };
    assert_converged(&root, "the crash-free reference", Ledger::ExactlyOnce).await?;

    // The claim this harness rests on: the task and the run are hosted by
    // different pods, so the task's owed run-creation and the run's result
    // proposal cross the wire rather than a function call. A shard-count or
    // hashing change that co-locates them takes that property away silently —
    // every other assertion here still passes with both entities on one pod.
    if pod_a.owns_task == pod_b.owns_task || pod_a.owns_run == pod_b.owns_run {
        return Err(error(format!(
            "each entity must be owned by exactly one pod; pod-a owns-task={} owns-run={}, \
             pod-b owns-task={} owns-run={}",
            pod_a.owns_task, pod_a.owns_run, pod_b.owns_task, pod_b.owns_run
        ))
        .into());
    }
    if pod_a.owns_task == pod_a.owns_run {
        return Err(error(
            "the task and the run landed on the same pod, so no exchange crosses the wire \
             and the sweep proves nothing this repository's in-process tests do not",
        )
        .into());
    }
    // The agent entity is hosted by exactly one pod, so exactly one pod's
    // command is a local ask and the other's crosses the wire. If the payload
    // codecs `init_agent_entity_remote_sharding` documents were not registered,
    // the remote arm cannot encode the command and this is what catches it —
    // the registration is otherwise made and never exercised, because every
    // other class is addressed by exchange envelope rather than by command.
    let arms = (pod_a.agent_command, pod_b.agent_command);
    if !matches!(
        arms,
        (AgentCommandArm::Local, AgentCommandArm::Remote)
            | (AgentCommandArm::Remote, AgentCommandArm::Local)
    ) {
        return Err(error(format!(
            "the agent entity's sharded command surface was not exercised across the wire: \
             pod-a took the {:?} arm and pod-b the {:?} arm, where exactly one must be Remote",
            arms.0, arms.1
        ))
        .into());
    }
    println!(
        "reference: two pods completed the task; pod-a wrote tasks={} runs={}, \
         pod-b wrote tasks={} runs={}; the agent entity was commanded across the wire",
        pod_a.task_writes, pod_a.run_writes, pod_b.task_writes, pod_b.run_writes
    );
    let _ = std::fs::remove_dir_all(&root);

    // Each row walks its write ordinals until the armed pod stops reaching
    // them. Taking the length from the reference run instead measures one
    // world and arms another: the two are shaped differently — only an armed
    // world ever loses a pod, takes over its shards, and recovers its
    // entities — and their write counts drift run to run on one machine
    // anyway. Ordinal `n` would then name whatever the reference's `n`th write
    // happened to be, and every ordinal past the armed world's own last write
    // would be a world that kills nothing and converges trivially. Here a
    // counted window is one whose pod left a crash marker, so it fired.
    let mut swept = Vec::new();
    let mut took_over = 0usize;
    for row in &SWEEP_ROWS {
        let (armed, target) = (row.armed, row.target);
        let mut fired = 0usize;
        let mut dry_streak = 0usize;
        let mut last_firing = 0usize;
        let mut gaps = 0usize;
        let mut nth = 1usize;
        while nth <= SWEEP_CEILING && dry_streak < DRY_ORDINALS_TO_STOP {
            let mut reached = false;
            for window in [PodCrash::BeforeWrite, PodCrash::AfterWrite] {
                let label = format!(
                    "{armed:?}-{}-{nth}-{}",
                    target.as_label(),
                    window.as_label()
                );
                let context = format!(
                    "{armed:?} killed at {} write {nth} ({})",
                    target.as_label(),
                    window.as_label()
                );
                let root = fresh_root(&label)?;
                match run_world(&root, Some((armed, target, nth, window))).await? {
                    World::Crashed(survivor) => {
                        assert_converged(&root, &context, Ledger::OneIdentity).await?;
                        if !survivor.terminal {
                            return Err(error(format!(
                                "{context}: the record converged but the surviving pod did not \
                                 see it, so it stopped for another reason"
                            ))
                            .into());
                        }
                        reached = true;
                        fired += 1;
                        if survivor.took_over {
                            took_over += 1;
                        }
                    }
                    // The flow never reached this write, so the world is a
                    // crash-free one. It still has to converge — but it is not
                    // a pod-loss window and is not counted as one.
                    World::Survived(_, _) => {
                        assert_converged(
                            &root,
                            &format!("{context}, which never fired"),
                            Ledger::ExactlyOnce,
                        )
                        .await?;
                    }
                    World::PortTaken => {
                        unreachable!("run_world retries or fails on a taken port")
                    }
                }
                let _ = std::fs::remove_dir_all(&root);
            }
            if reached {
                // Every ordinal skipped since the last firing one was a miss
                // this row walked past, not the end of the row.
                gaps += nth - last_firing - 1;
                last_firing = nth;
                dry_streak = 0;
            } else {
                dry_streak += 1;
            }
            nth += 1;
        }
        if dry_streak < DRY_ORDINALS_TO_STOP {
            println!(
                "note: {armed:?} {} stopped at the {SWEEP_CEILING}-write ceiling rather than at \
                 the flow's last write; windows past it were not swept",
                target.as_label()
            );
        }
        if fired == 0 {
            if let Some(because) = row.unreachable_because {
                println!(
                    "  {armed:?} {}: 0 windows — unreachable: {because}",
                    target.as_label()
                );
                swept.push(fired);
                continue;
            }
        }
        let gap_note = if gaps == 0 {
            String::new()
        } else {
            format!(", {gaps} ordinal(s) missed and walked past")
        };
        println!(
            "  {armed:?} {}: {fired} windows, ordinals 1-{last_firing}{gap_note}",
            target.as_label()
        );
        if let Some(floor) = row.floor {
            if fired < floor {
                return Err(error(format!(
                    "{armed:?} {} fired {fired} windows, below its floor of {floor}: the row \
                     collapsed rather than running short",
                    target.as_label()
                ))
                .into());
            }
        } else if fired > 0 {
            println!(
                "note: {armed:?} {} was expected to be unreachable and fired {fired} windows; \
                 the sweep gained coverage and the note above is stale",
                target.as_label()
            );
        }
        swept.push(fired);
    }

    // The rows no single-arming world can reach: the second owner's own writes,
    // to each store it touches after inheriting the shard.
    let mut takeover = 0usize;
    for (target, floor) in [
        (CrashTarget::Runs, TAKEOVER_RUNS_FLOOR),
        (CrashTarget::Workflow, TAKEOVER_WORKFLOW_FLOOR),
    ] {
        takeover += sweep_second_owner(target, floor).await?;
    }
    swept.push(takeover);
    // Every one of these moved a shard, twice: the second owner inherited the
    // run from the first, and the replacement inherited it from the second and
    // could not have converged without downing both.
    took_over += takeover;

    // No `windows == 0` guard: every reachable row has already been held to its
    // own floor above, which is strictly stronger. A total of zero now fails
    // naming the row that collapsed rather than the sweep as a whole.
    let windows: usize = swept.iter().sum();

    // A crash marker says a pod died, not that a shard moved. A window whose
    // armed pod died after finishing its own part is a real recovery — the
    // survivor completes from the shared record — but nothing was taken over in
    // it, so it exercises neither the downing, the shard movement, nor the
    // re-materialization on another pod. Some window has to reach those, or the
    // sweep proves only the cheaper half of what this harness claims.
    if took_over < TAKEOVER_FLOOR {
        return Err(error(format!(
            "only {took_over} of {windows} windows moved a shard to the surviving pod, below \
             the floor of {TAKEOVER_FLOOR}: the sweep is killing pods without exercising \
             recovery on another pod"
        ))
        .into());
    }

    println!(
        "swept {windows} pod-loss windows; every one fired and converged from the shared record \
         ({took_over} moved a shard to the survivor, {} were finished by the surviving owner \
         without one)",
        windows - took_over
    );
    Ok(())
}
