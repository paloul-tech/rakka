//! Small shared helpers, constants, and error type for the example.

use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub type ExampleError = Box<dyn Error + Send + Sync>;
pub type ExampleResult<T> = Result<T, ExampleError>;

/// Sharded entity type that hosts one durable run per run id.
pub const ENTITY_TYPE: &str = "AgentRun";

/// Number of shards distributed across cluster nodes for run ownership.
pub const NUMBER_OF_SHARDS: u32 = 64;

/// Default Rakka TCP remoting port for inter-node communication.
///
/// This is a real listening port: nodes talk to each other over `rakka-remote`
/// TCP, not HTTP. The logical node id is derived from it so it stays stable
/// across restarts on the same port.
pub const DEFAULT_RAKKA_PORT: u16 = 25530;

/// How often each process republishes discovery and advances membership.
pub const DEFAULT_DISCOVERY_POLL: Duration = Duration::from_millis(750);

/// How long a discovery record is trusted after its last update.
pub const DEFAULT_DISCOVERY_TTL: Duration = Duration::from_secs(30);

/// TCP remoting connect timeout.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

/// TCP remoting reconnect backoff.
pub const DEFAULT_RECONNECT_BACKOFF: Duration = Duration::from_millis(25);

/// TCP remoting idle connection timeout.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout for run ask/remote-ask requests.
pub const RUN_ASK_TIMEOUT: Duration = Duration::from_secs(5);

/// Workflow type label shared by the demo workflow definition and every plan.
pub const WORKFLOW_TYPE: &str = "compiled-graph-demo";

/// Default etcd key prefix under which members register themselves.
pub const DEFAULT_ETCD_PREFIX: &str = "/rakka/agent-workflow/members/";

/// Default etcd lease TTL (seconds) for a member's registration key.
///
/// A member renews its lease every poll interval; if it dies, etcd deletes the
/// key after the lease lapses, which is how scale-in/crashes leave membership.
pub const DEFAULT_ETCD_LEASE_TTL_SECONDS: i64 = 10;

/// Reads an environment variable as a `u16`, falling back to `default`.
pub fn env_u16(name: &str, default: u16) -> ExampleResult<u16> {
    env::var(name)
        .ok()
        .map(|value| parse_u16(name, &value))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

/// Parses a `u16` with a contextual error message.
pub fn parse_u16(name: &str, value: &str) -> ExampleResult<u16> {
    value
        .parse::<u16>()
        .map_err(|error| example_error(format!("{name} must be a port number: {error}")).into())
}

/// Builds the default, restart-stable logical node id for a Rakka port.
pub fn default_node_logical_id(rakka_port: u16) -> String {
    format!("agent-node-{rakka_port}")
}

/// Builds a fresh per-process node incarnation so a restarted process is not
/// mistaken for the old incarnation a peer may have already marked down.
pub fn default_node_incarnation(rakka_port: u16) -> String {
    format!(
        "uid-{rakka_port}-{}-{}",
        current_timestamp_millis(),
        std::process::id()
    )
}

/// Current wall-clock time in milliseconds since the Unix epoch.
pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Converts a duration to whole milliseconds, saturating on overflow.
pub fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Lowercase hex encoding used for filesystem-safe record names.
pub fn hex_encode(value: &str) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

/// Stable FNV-1a 64-bit hash, used to fingerprint compiled plans deterministically.
pub fn stable_hash(value: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Builds a simple `std::io::Error` from a message for example error paths.
pub fn example_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}
