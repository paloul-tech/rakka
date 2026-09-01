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
///
/// The count is exact rather than a floor. `>= 5` against six configured keys
/// left one policy's worth of slack, which is enough to delete a class — or to
/// quote a key so the scan mis-parses it — with every assertion here still
/// green.
#[test]
fn every_retention_policy_selects_on_an_attribute_the_adapter_writes() {
    let policies = string_attribute_policies();
    let keys: BTreeSet<&str> = policies.iter().map(|policy| policy.key.as_str()).collect();
    assert_eq!(
        keys.len(),
        6,
        "the topology configures six distinct string-attribute selectors; the scan \
         found {keys:?}, so either a class was dropped or the scan mis-parsed one"
    );
    for key in &keys {
        assert!(
            rakka_agent::otel::is_agent_span_attribute(key),
            "the tail-sampling policy on `{key}` selects on an attribute no mapping \
             function writes: it would retain nothing in production"
        );
    }
}

/// Every retention policy selects on **values** the adapter actually writes.
///
/// The key is only half of a selector. `error.type: ["rakka.agent.authority"]`
/// retains the security-denial class exactly as long as that string is one
/// `AGENT_SEGMENT_ERROR_TYPES` contains — rename the variant's label in Rust,
/// or typo the value here, and the class retains nothing in production while
/// the key assertion above stays green. That is the same defect four of the
/// eight classes already had once, one level down.
///
/// Only the closed vocabularies are checked. `rakka.error.code` and
/// `rakka.agent.effect.attempt` select by regex over open sets, and
/// `rakka.agent.settings_revision` takes a deployment-supplied revision from
/// the environment; asserting those against a fixed list would be asserting a
/// guess.
#[test]
fn every_retention_policy_selects_on_values_the_adapter_writes() {
    let mut checked = 0;
    for policy in string_attribute_policies() {
        let vocabulary: Vec<&str> = match policy.key.as_str() {
            "error.type" => rakka_agent::AGENT_SEGMENT_ERROR_TYPES.to_vec(),
            "rakka.agent.effect.status" => AGENT_EFFECT_STATUS_LABELS.to_vec(),
            "rakka.agent.checkpoint.kind" => AGENT_CHECKPOINT_KIND_LABELS.to_vec(),
            _ => continue,
        };
        for value in &policy.values {
            assert!(
                vocabulary.contains(&value.as_str()),
                "the `{}` policy retains on `{}`, which `{}`'s vocabulary does not \
                 contain: the class would retain nothing in production. Vocabulary: \
                 {vocabulary:?}",
                policy.name,
                value,
                policy.key
            );
            checked += 1;
        }
    }
    assert_eq!(
        checked, 8,
        "four policies select on three closed vocabularies — `error.type` twice,
         for the security-denial and recovery classes — for eight values \
         between them; a different count means a policy was added, removed, or \
         mis-parsed"
    );
}

/// `AgentRunEffectStatus::as_label` over every variant.
const AGENT_EFFECT_STATUS_LABELS: &[&str] = &[
    "pending",
    "ready",
    "succeeded",
    "failed",
    "exhausted",
    "indeterminate",
    "compensated",
    "cancelled",
];

/// `AgentCheckpointKind::as_label` over every variant.
const AGENT_CHECKPOINT_KIND_LABELS: &[&str] = &[
    "approval",
    "security-authorization",
    "indeterminate-effect-reconciliation",
];

/// The two label lists above are the enums', not a copy that can drift.
///
/// A hand-written list is what this whole suite exists to prevent, so it is
/// checked against the enum it transcribes — every variant, in both
/// directions.
#[test]
fn the_transcribed_label_vocabularies_match_their_enums() {
    use rakka_agent::{AgentCheckpointKind, AgentRunEffectStatus};

    let statuses: BTreeSet<&str> = [
        AgentRunEffectStatus::Pending,
        AgentRunEffectStatus::Ready,
        AgentRunEffectStatus::Succeeded,
        AgentRunEffectStatus::Failed,
        AgentRunEffectStatus::Exhausted,
        AgentRunEffectStatus::Indeterminate,
        AgentRunEffectStatus::Compensated,
        AgentRunEffectStatus::Cancelled,
    ]
    .into_iter()
    .map(AgentRunEffectStatus::as_label)
    .collect();
    assert_eq!(
        statuses,
        AGENT_EFFECT_STATUS_LABELS.iter().copied().collect(),
        "AGENT_EFFECT_STATUS_LABELS has drifted from AgentRunEffectStatus::as_label"
    );

    let kinds: BTreeSet<&str> = [
        AgentCheckpointKind::Approval,
        AgentCheckpointKind::SecurityAuthorization,
        AgentCheckpointKind::IndeterminateEffectReconciliation,
    ]
    .into_iter()
    .map(AgentCheckpointKind::as_label)
    .collect();
    assert_eq!(
        kinds,
        AGENT_CHECKPOINT_KIND_LABELS.iter().copied().collect(),
        "AGENT_CHECKPOINT_KIND_LABELS has drifted from AgentCheckpointKind::as_label"
    );
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
    for policy in string_attribute_policies() {
        let key = policy.key;
        assert!(
            allowlisted.contains(&key),
            "`{key}` is a retention selector but the span allowlist deletes it \
             before the sampler runs"
        );
    }
}

/// The metric allowlist is conditioned on agent-domain metrics; spans and logs
/// are not.
///
/// A `keep_keys` with no `where` deletes every key it does not name from
/// **every** datapoint in the pipeline, and this list is the *agent* label
/// vocabulary. The application's bridge exports the whole `MetricsSnapshot`,
/// so the unconditioned rule was stripping `state` off `rakka.cluster.members`
/// and `actor` off `rakka.actor.mailbox.depth` — collapsing each into one
/// attribute-less last-write-wins series, which is destruction rather than
/// redaction.
///
/// The other two are asserted **unconditioned**, because that is equally
/// deliberate: the log list is the last guard over `tracing` records whose
/// fields are arbitrary, and those reach the Collector under a scope named for
/// their target rather than the pinned one, so any scope condition would
/// exempt precisely the records that most need filtering.
#[test]
fn the_metric_allowlist_applies_only_to_agent_metrics() {
    let statement = statement_for("metric_statements");
    assert!(
        statement.contains("where IsMatch(metric.name,"),
        "the metric keep_keys runs unconditioned, so it deletes the labels of \
         every non-agent instrument the bridge exports: {statement}"
    );
    for name in AGENT_DOMAIN_METRIC_NAMES {
        assert!(
            name.starts_with("rakka.agent."),
            "`{name}` is a catalogued agent instrument the condition's \
             `^rakka\\.agent\\.` prefix would exclude"
        );
    }
    for context in ["trace_statements", "log_statements"] {
        assert!(
            !statement_for(context).contains(" where "),
            "`{context}` is conditioned; the span and log vocabularies are \
             closed sets the adapter writes, and the log rule is the last guard \
             over `tracing` records that do not carry the pinned scope"
        );
    }
}

/// Every catalogued agent-domain instrument name.
const AGENT_DOMAIN_METRIC_NAMES: &[&str] = &[
    "rakka.agent.decisions",
    "rakka.agent.effect.outcomes",
    "rakka.agent.effect.outstanding.duration",
    "rakka.agent.model.tokens",
    "rakka.agent.recovery.duration",
    "rakka.agent.run.transitions",
    "rakka.agent.telemetry.export.queue",
    "rakka.agent.turn.duration",
];

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

/// The gateway ConfigMap's `collector.yaml`, and only that.
///
/// **Every scan below is anchored here, and that is not tidiness.** The
/// helpers used to split the whole 631-line file on the first
/// `        trace_statements:\n` they found. The allowlist lives in the
/// gateway tier today and nowhere else, so that happened to be right — and
/// would have stayed right until the day the DaemonSet tier grew any
/// `transform/*` of its own, at which point every assertion in this file would
/// have silently moved to the wrong document and the gateway's allowlist would
/// have gone unverified with all six tests green.
fn gateway_config() -> &'static str {
    const MARKER: &str = "  name: rakka-agent-otel-gateway-config\n";
    let documents: Vec<&str> = TOPOLOGY.split("\n---\n").collect();
    let mut matching = documents
        .iter()
        .filter(|document| document.contains(MARKER));
    let document = matching
        .next()
        .expect("the topology declares a gateway ConfigMap");
    assert!(
        matching.next().is_none(),
        "two documents claim to be the gateway ConfigMap"
    );
    document
        .split_once("  collector.yaml: |\n")
        .map(|(_, payload)| payload)
        .expect("the gateway ConfigMap carries a collector.yaml")
}

/// The keys of one `keep_keys(attributes, [...])` statement.
fn keep_keys(context: &str) -> BTreeSet<String> {
    let config = gateway_config();
    let anchor = format!("        {context}:\n");
    assert_eq!(
        config.matches(anchor.as_str()).count(),
        1,
        "`{context}` appears more than once in the gateway configuration, so \
         reading the first is a guess"
    );
    let section = config
        .split_once(anchor.as_str())
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

/// One transform statement, from its `keep_keys(` to the end of its block.
fn statement_for(context: &str) -> String {
    let config = gateway_config();
    let anchor = format!("        {context}:\n");
    let section = config
        .split_once(anchor.as_str())
        .map(|(_, rest)| rest)
        .unwrap_or_else(|| panic!("the allowlist has no `{context}`"));
    // Every line more deeply indented than the `        <context>:` key that
    // opened it — a statement's `where` clause sits on its own continuation
    // line, so reading only the `keep_keys(...)` call would miss exactly what
    // this is here to check.
    section
        .lines()
        .take_while(|line| line.trim().is_empty() || line.len() - line.trim_start().len() > 8)
        .collect::<Vec<_>>()
        .join("\n")
}

/// One `string_attribute` retention policy, as configured.
struct RetentionPolicy {
    name: String,
    key: String,
    values: Vec<String>,
}

/// Every `string_attribute` retention policy, with the values it selects on.
///
/// Parsed per policy block rather than by splitting on `string_attribute:`
/// across the whole list, so a key and the values beside it can never come
/// from two different policies.
fn string_attribute_policies() -> Vec<RetentionPolicy> {
    let config = gateway_config();
    let policies = config
        .split_once("        policies:\n")
        .map(|(_, rest)| rest)
        .expect("the gateway declares tail-sampling policies");
    let end = policies
        .find("\n      batch:")
        .expect("the policy list is followed by the batch processor");
    policies[..end]
        .split("          - name: ")
        .skip(1)
        .filter(|block| block.contains("            string_attribute:\n"))
        .map(|block| {
            let name = block.lines().next().unwrap_or_default().trim().to_string();
            let body = block
                .split_once("            string_attribute:\n")
                .map(|(_, rest)| rest)
                .unwrap_or_else(|| panic!("`{name}` declares no string_attribute body"));
            let key = body
                .split_once("              key: ")
                .map(|(_, rest)| rest.lines().next().unwrap_or_default().trim().to_string())
                .unwrap_or_else(|| panic!("`{name}` selects on no key"));
            RetentionPolicy {
                values: policy_values(&name, body),
                name,
                key,
            }
        })
        .collect()
}

/// The `values:` of one policy, inline (`["a", "b"]`) or as a block list.
fn policy_values(name: &str, body: &str) -> Vec<String> {
    let rest = body
        .split_once("              values:")
        .map(|(_, rest)| rest)
        .unwrap_or_else(|| panic!("`{name}` selects on no values"));
    let first = rest.lines().next().unwrap_or_default().trim();
    if first.starts_with('[') {
        return quoted(first).into_iter().collect();
    }
    rest.lines()
        .skip(1)
        .take_while(|line| line.trim_start().starts_with("- "))
        .map(|line| line.trim().trim_start_matches("- ").trim())
        .flat_map(quoted)
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
