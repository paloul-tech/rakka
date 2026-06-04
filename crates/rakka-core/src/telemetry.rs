//! Tracing conventions shared across Rakka crates.

pub use tracing::Level;

/// Trace target prefix used by all framework spans and events.
pub const TRACE_TARGET_PREFIX: &str = "rakka";

/// Builds a trace target for a framework component.
#[must_use]
pub fn component_target(component: &str) -> String {
    format!("{TRACE_TARGET_PREFIX}.{component}")
}
