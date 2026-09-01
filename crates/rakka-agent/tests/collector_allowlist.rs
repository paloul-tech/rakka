//! The Collector's allowlist and its retention policies, held to the code.
//!
//! Specification: [17.14](../../../docs/plans/rakka-agent/spec.md) (the
//! application minimises before export and the Collector allowlists as defence
//! in depth), [17.16](../../../docs/plans/rakka-agent/spec.md) (retention
//! classes), [17.20](../../../docs/plans/rakka-agent/spec.md) (an upgrade
//! reviews Collector rules).
//!
//! **Why this suite is here and not in `rakka-k8s`.** The topology's
//! Kubernetes shape is contract-tested in
//! `crates/rakka-k8s/tests/agent_otel_collector_topology.rs`, but `rakka-k8s`
//! sits below `rakka-agent` in the crate DAG and cannot see
//! `AGENT_SPAN_ATTRIBUTE_KEYS`. Asserting the YAML's key list against a copy
//! of that list would be a configuration validated against itself: it passes
//! forever, including on the day someone adds a span attribute and the
//! Collector starts silently deleting it. So the bijection lives here, where
//! the constants do, and reads the same file.

#![cfg(feature = "otel")]

use std::collections::BTreeSet;

use rakka_agent::otel::{AGENT_LOG_ATTRIBUTE_KEYS, AGENT_SPAN_ATTRIBUTE_KEYS};
use rakka_agent::AGENT_METRIC_FIELDS;

const TOPOLOGY: &str =
    include_str!("../../../docs/plans/rakka-agent/kubernetes-agent-otel-collector-topology.yaml");

/// The span allowlist is exactly the keys the adapter may write on a span.
#[test]
fn the_span_allowlist_is_the_span_vocabulary() {
    let configured = keep_keys("trace_statements");
    let declared: BTreeSet<String> = AGENT_SPAN_ATTRIBUTE_KEYS
        .iter()
        .map(|key| (*key).to_string())
        .collect();
    assert_bijective(&configured, &declared, "span");
}

/// The log allowlist is the span vocabulary unioned with the durable
/// correlation identities.
///
/// The union, not one or the other: `is_agent_log_attribute` accepts both, and
/// a log record legitimately carries the identities 17.13 asks a structured log
/// to carry — which are exactly the identities 17.12 forbids on a metric.
/// Applying the span list alone would strip the audit trail while calling it
/// redaction.
#[test]
fn the_log_allowlist_is_the_wider_log_vocabulary() {
    let configured = keep_keys("log_statements");
    let declared: BTreeSet<String> = AGENT_SPAN_ATTRIBUTE_KEYS
        .iter()
        .chain(AGENT_LOG_ATTRIBUTE_KEYS.iter())
        .map(|key| (*key).to_string())
        .collect();
    assert_bijective(&configured, &declared, "log");
    for key in &configured {
        assert!(
            rakka_agent::otel::is_agent_log_attribute(key),
            "`{key}` is allowlisted at the Collector but the adapter would filter it"
        );
    }
}

/// The metric datapoint allowlist is the bounded label vocabulary.
#[test]
fn the_metric_allowlist_is_the_bounded_label_vocabulary() {
    let configured = keep_keys("metric_statements");
    let declared: BTreeSet<String> = AGENT_METRIC_FIELDS
        .iter()
        .chain(rakka_agent_workflow::AGENT_WORKFLOW_BOUNDED_METRIC_ATTRIBUTES.iter())
        .chain(rakka_agent_workflow::BOUNDED_METRIC_FIELDS.iter())
        .map(|key| (*key).to_string())
        .collect();
    assert_bijective(&configured, &declared, "metric datapoint");
    for key in &configured {
        assert!(
            !rakka_agent_workflow::is_forbidden_agent_metric_attribute(key),
            "`{key}` is allowlisted for metric datapoints but is a forbidden hot label"
        );
    }
}

/// Every retention policy selects on an attribute a mapping function writes.
///
/// This is the assertion the slice exists for. A `tail_sampling` policy keyed
/// on an attribute nothing emits retains nothing in production while passing
/// every string assertion about the YAML — and four of 17.16's eight classes
/// were in exactly that position until slice 6.3a's follow-up pass added
/// `rakka.agent.effect.status`, `.effect.attempt`, `.checkpoint.kind`, and
/// `.settings_revision` to the segments that know them.
#[test]
fn every_retention_policy_selects_on_an_attribute_the_adapter_writes() {
    let keys = policy_attribute_keys();
    assert!(
        keys.len() >= 5,
        "the scan found {} policy attributes, so it is proving little",
        keys.len()
    );
    for key in &keys {
        assert!(
            rakka_agent::otel::is_agent_span_attribute(key),
            "the tail-sampling policy on `{key}` selects on an attribute no mapping \
             function writes: it would retain nothing in production"
        );
    }
}

/// A policy key that survives the allowlist is one the sampler can still see.
///
/// Order matters and the topology runs `transform/allowlist` before
/// `tail_sampling`, so a key stripped by the allowlist is invisible to every
/// policy that selects on it. The two lists are checked against each other
/// here rather than assumed compatible.
#[test]
fn no_retention_policy_selects_on_a_key_the_allowlist_strips() {
    let allowlisted = keep_keys("trace_statements");
    for key in policy_attribute_keys() {
        assert!(
            allowlisted.contains(&key),
            "`{key}` is a retention selector but the span allowlist deletes it \
             before the sampler runs"
        );
    }
}

/// The pinned convention revision the manifests document is the one the
/// adapter stamps.
#[test]
fn the_documented_convention_revision_is_the_pinned_one() {
    let readme =
        include_str!("../../../docs/plans/rakka-agent/kubernetes-agent-otel-collector-topology.md");
    assert!(
        readme.contains(rakka_agent::otel::AGENT_GENAI_CONVENTION_REVISION),
        "the topology documents a convention revision the adapter does not stamp"
    );
}

/// Both directions of a set equality, reported by what is missing from which.
fn assert_bijective(configured: &BTreeSet<String>, declared: &BTreeSet<String>, what: &str) {
    for key in configured {
        assert!(
            declared.contains(key),
            "the Collector's {what} allowlist keeps `{key}`, which no {what} \
             vocabulary declares"
        );
    }
    for key in declared {
        assert!(
            configured.contains(key),
            "`{key}` is a declared {what} attribute the Collector's allowlist \
             deletes, so it would never reach a backend"
        );
    }
}

/// The keys of one `keep_keys(attributes, [...])` statement.
fn keep_keys(context: &str) -> BTreeSet<String> {
    let section = TOPOLOGY
        .split_once(&format!("        {context}:\n"))
        .map(|(_, rest)| rest)
        .unwrap_or_else(|| panic!("the allowlist has no `{context}`"));
    let statement = section
        .split_once("keep_keys(attributes, [")
        .map(|(_, rest)| rest)
        .unwrap_or_else(|| panic!("`{context}` runs no keep_keys"));
    let body = statement
        .split_once("])")
        .map(|(body, _)| body)
        .unwrap_or_else(|| panic!("`{context}`'s keep_keys is unterminated"));
    quoted(body)
}

/// Every `key:` a `string_attribute` retention policy selects on.
fn policy_attribute_keys() -> BTreeSet<String> {
    let policies = TOPOLOGY
        .split_once("        policies:\n")
        .map(|(_, rest)| rest)
        .expect("the gateway declares tail-sampling policies");
    let end = policies
        .find("\n      batch:")
        .expect("the policy list is followed by the batch processor");
    policies[..end]
        .split("string_attribute:")
        .skip(1)
        .filter_map(|chunk| {
            chunk
                .split_once("key: ")
                .map(|(_, rest)| rest.lines().next().unwrap_or("").trim().to_string())
        })
        .filter(|key| !key.is_empty())
        .collect()
}

/// Every double-quoted token in a fragment.
fn quoted(body: &str) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    let mut rest = body;
    while let Some((_, after)) = rest.split_once('"') {
        let Some((key, tail)) = after.split_once('"') else {
            break;
        };
        keys.insert(key.to_string());
        rest = tail;
    }
    keys
}
