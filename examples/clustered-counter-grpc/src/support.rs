//! Small shared helpers for the example.

use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub type ExampleError = Box<dyn Error + Send + Sync>;
pub type ExampleResult<T> = Result<T, ExampleError>;

pub const ENTITY_TYPE: &str = "Counter";
pub const DEFAULT_RAKKA_TCP_PORT: u16 = 25520;
pub const DEFAULT_DISCOVERY_POLL: Duration = Duration::from_millis(750);
pub const DEFAULT_DISCOVERY_TTL: Duration = Duration::from_secs(30);
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);
pub const DEFAULT_RECONNECT_BACKOFF: Duration = Duration::from_millis(25);
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

pub fn env_u16(name: &str, default: u16) -> ExampleResult<u16> {
    env::var(name)
        .ok()
        .map(|value| parse_u16(name, &value))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

pub fn parse_u16(name: &str, value: &str) -> ExampleResult<u16> {
    value
        .parse::<u16>()
        .map_err(|error| example_error(format!("{name} must be a TCP port: {error}")).into())
}

pub fn default_node_incarnation(tcp_port: u16) -> String {
    format!(
        "uid-{tcp_port}-{}-{}",
        current_timestamp_millis(),
        std::process::id()
    )
}

pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

pub fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub fn hex_encode(value: &str) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

pub fn hex_decode(value: &str) -> Option<String> {
    if value.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks_exact(2) {
        let text = std::str::from_utf8(chunk).ok()?;
        let byte = u8::from_str_radix(text, 16).ok()?;
        bytes.push(byte);
    }
    String::from_utf8(bytes).ok()
}

pub fn example_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}
