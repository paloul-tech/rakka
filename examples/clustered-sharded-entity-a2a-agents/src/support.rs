//! Small shared helpers, constants, and error type for the example.

use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Boxed error type used by the example.
pub type ExampleError = Box<dyn Error + Send + Sync>;

/// Result type used by the example.
pub type ExampleResult<T> = Result<T, ExampleError>;

/// Default Rakka TCP remoting port for local development.
pub const DEFAULT_RAKKA_PORT: u16 = 25_580;

/// Default etcd discovery key prefix for production-like testing.
pub const DEFAULT_ETCD_PREFIX: &str = "/rakka/examples/a2a-agents";

/// Default etcd lease TTL in seconds.
pub const DEFAULT_ETCD_LEASE_TTL_SECONDS: i64 = 10;

/// How often each process republishes file-discovery membership.
pub const DEFAULT_DISCOVERY_POLL: Duration = Duration::from_millis(750);

/// How long a discovery record is trusted after its last update.
pub const DEFAULT_DISCOVERY_TTL: Duration = Duration::from_secs(30);

/// TCP remoting connect timeout.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

/// TCP remoting reconnect backoff.
pub const DEFAULT_RECONNECT_BACKOFF: Duration = Duration::from_millis(25);

/// TCP remoting idle timeout.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Bounded ask timeout for sharded owner and local run actor requests.
pub const RUN_ASK_TIMEOUT: Duration = Duration::from_secs(3);

/// How long an idle run entity stays resident before passivating.
///
/// Passivation keeps read-only probes for arbitrary task ids from pinning an
/// entity and child run actor forever; durable state recovers lazily on the
/// next reference.
pub const RUN_ENTITY_IDLE_PASSIVATION: Duration = Duration::from_secs(120);

/// Bounded workflow type label used by the demo workflow.
pub const WORKFLOW_TYPE: &str = "a2a-phase-2-demo";

/// Reads an environment variable as a `u16`, falling back to `default`.
pub fn env_u16(name: &str, default: u16) -> ExampleResult<u16> {
    env::var(name)
        .ok()
        .map(|value| parse_u16(name, &value))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

/// Reads an environment variable as a `u64`, falling back to `default`.
pub fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

/// Parses a `u16` with a contextual error message.
pub fn parse_u16(name: &str, value: &str) -> ExampleResult<u16> {
    value
        .parse::<u16>()
        .map_err(|error| example_error(format!("{name} must be a port number: {error}")).into())
}

/// Builds the default logical node id for a Rakka port.
pub fn default_node_logical_id(rakka_port: u16) -> String {
    format!("a2a-agent-node-{rakka_port}")
}

/// Builds a fresh per-process node incarnation.
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

/// Lowercase hex encoding used for filesystem-safe discovery record names.
pub fn hex_encode(value: &str) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

/// Inverse of [`hex_encode`]; returns `None` for malformed input.
pub fn hex_decode(value: &str) -> Option<String> {
    if value.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks_exact(2) {
        let pair = std::str::from_utf8(chunk).ok()?;
        bytes.push(u8::from_str_radix(pair, 16).ok()?);
    }
    String::from_utf8(bytes).ok()
}

/// Builds a simple `std::io::Error` from a message for example error paths.
pub fn example_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}
