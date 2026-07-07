//! Crate-internal shared helpers.

use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use rakka_agent_workflow::AgentTimestampMillis;

/// Current wall-clock time in milliseconds since the Unix epoch.
pub(crate) fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Current wall-clock time as an agent timestamp.
// Consumed by the request handler and push modules from Slice 7.4/7.7 on.
#[allow(dead_code)]
pub(crate) fn now_timestamp() -> AgentTimestampMillis {
    AgentTimestampMillis::new(current_timestamp_millis())
}

/// Lowercase hex encoding used for separator-safe persistence-id parts.
// Consumed by the push config module from Slice 7.7 on.
#[allow(dead_code)]
pub(crate) fn hex_encode(value: &str) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_encoding_is_lowercase_and_reversible_by_inspection() {
        assert_eq!(hex_encode("a:b"), "613a62");
        assert_eq!(hex_encode(""), "");
    }
}
