//! Metric names, bounded labels, and operational snapshots.
//!
//! Snapshots expose production-review state; they are never a correctness
//! source. Metric labels are bounded and must never include task ids, actor
//! paths, prompts, callback URLs, payloads, command args, temp paths, full
//! errors, or secrets — only the low-cardinality labels enumerated here.

/// Counter of A2A ingress requests, labeled by operation.
pub const METRIC_INGRESS: &str = "rakka.a2a.ingress";
/// Counter of durable command acceptances (post-inbox).
pub const METRIC_DURABLE_ACCEPTED: &str = "rakka.a2a.durable_accepted";
/// Counter of duplicate/conflict/rejection outcomes, labeled by outcome.
pub const METRIC_COMMAND_OUTCOME: &str = "rakka.a2a.command_outcome";
/// Counter of stream lifecycle events, labeled by phase.
pub const METRIC_STREAM: &str = "rakka.a2a.stream";
/// Counter of task-projection operations, labeled by kind.
pub const METRIC_PROJECTION: &str = "rakka.a2a.projection";
/// Counter of push-delivery outcomes, labeled by outcome.
pub const METRIC_PUSH_DELIVERY: &str = "rakka.a2a.push_delivery";
/// Counter of owner-routing outcomes, labeled by result.
pub const METRIC_OWNER_ROUTING: &str = "rakka.a2a.owner_routing";
/// Counter of adapter errors, labeled by stable error code.
pub const METRIC_ERROR: &str = "rakka.a2a.error";

/// Bounded metric label keys used by the crate's instruments.
///
/// Only these keys may label A2A adapter metrics; anything else risks
/// unbounded cardinality or leaking sensitive values.
pub const BOUNDED_METRIC_LABELS: &[&str] = &[
    "operation",
    "outcome",
    "phase",
    "kind",
    "result",
    "code",
    "tenant",
    "transport",
];

/// Returns true when `key` is an allowed bounded metric label.
#[must_use]
pub fn is_bounded_metric_label(key: &str) -> bool {
    BOUNDED_METRIC_LABELS.contains(&key)
}

/// Keys that must never appear as A2A metric labels.
const FORBIDDEN_METRIC_LABELS: &[&str] = &[
    "task_id",
    "run_id",
    "context_id",
    "message_id",
    "actor_path",
    "url",
    "callback_url",
    "prompt",
    "payload",
    "command_args",
    "temp_path",
    "error_message",
    "stacktrace",
    "token",
    "credentials",
];

/// Returns true when `key` must never be used as a metric label.
#[must_use]
pub fn is_forbidden_metric_label(key: &str) -> bool {
    FORBIDDEN_METRIC_LABELS.contains(&key)
}

/// Aggregated operational snapshot for a running A2A adapter.
///
/// Composed from the sub-snapshots each subsystem already exposes. Used for
/// production review dashboards and readiness inspection; never authoritative.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct A2AAdapterSnapshot {
    /// Whether mutating public ingress is currently accepted.
    pub accepting_public_commands: bool,
    /// Bounded stream metrics.
    #[cfg(feature = "server")]
    pub streams: crate::stream::A2AStreamMetricsSnapshot,
    /// Bounded push-delivery metrics, when a dispatcher is configured.
    pub push_delivery: Option<crate::dispatch::A2APushDispatchSnapshot>,
    /// Task projection backend name.
    pub projection_backend: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_allow_and_deny_lists_are_disjoint_and_correct() {
        assert!(is_bounded_metric_label("operation"));
        assert!(is_bounded_metric_label("code"));
        assert!(!is_bounded_metric_label("task_id"));

        assert!(is_forbidden_metric_label("task_id"));
        assert!(is_forbidden_metric_label("callback_url"));
        assert!(is_forbidden_metric_label("token"));

        for label in BOUNDED_METRIC_LABELS {
            assert!(
                !is_forbidden_metric_label(label),
                "bounded label {label} must not also be forbidden"
            );
        }
    }
}
