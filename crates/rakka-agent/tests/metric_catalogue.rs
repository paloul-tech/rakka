//! The agent-domain metric catalogue, and its bijection with the code.
//!
//! Specification: [17.12](../../../docs/plans/rakka-agent/spec.md) — "Metric
//! labels MUST be bounded **and documented**." A prose table alone drifts from
//! the call sites the moment one changes, which is exactly what happened: the
//! aspirational table in `docs/plans/rakka-agent/technical-guidance.md` names
//! fifteen metrics that do not exist, and the in-crate label list this suite's
//! sibling replaced had gone stale on four keys that were already being
//! recorded.
//!
//! So the catalogue is data — `AGENT_DOMAIN_METRIC_INSTRUMENTS` — and this
//! suite makes it impossible for the data and the code to disagree: every
//! `METRIC_AGENT_*` constant the crate defines must be catalogued, and every
//! catalogued name must be a constant the crate defines. Adding a metric
//! without documenting it fails here rather than shipping undocumented.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use rakka_agent::{
    agent_domain_instrument_views, agent_domain_metric_instrument,
    validate_agent_domain_metric_attributes, AGENT_DOMAIN_METRIC_INSTRUMENTS,
    AGENT_SEGMENT_ERROR_TYPES, AGENT_TELEMETRY_SIGNALS,
};

fn crate_src() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

/// Every string literal a `pub const METRIC_AGENT_…: &str = "…";` in this
/// crate's sources binds, paired with the constant's identifier.
fn declared_metric_names() -> Vec<(String, String)> {
    const MARKER: &str = "pub const METRIC_AGENT_";
    let mut declared = Vec::new();
    let entries = fs::read_dir(crate_src()).expect("the crate's src directory is readable");
    for entry in entries {
        let path = entry.expect("a directory entry is readable").path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("a source file is readable");
        for chunk in source.split(MARKER).skip(1) {
            let statement = chunk
                .split_once(';')
                .map(|(statement, _)| statement)
                .unwrap_or(chunk);
            let identifier = statement
                .split_once(':')
                .map(|(identifier, _)| identifier.trim())
                .expect("a constant declares a type");
            let value = statement
                .split_once('"')
                .and_then(|(_, rest)| rest.split_once('"'))
                .map(|(value, _)| value)
                .expect("a metric constant binds a string literal");
            declared.push((format!("METRIC_AGENT_{identifier}"), value.to_string()));
        }
    }
    assert!(
        !declared.is_empty(),
        "the scan found no metric constants at all, so it is proving nothing"
    );
    declared
}

#[test]
fn every_metric_the_crate_declares_is_catalogued() {
    let catalogued: BTreeSet<&str> = AGENT_DOMAIN_METRIC_INSTRUMENTS
        .iter()
        .map(|instrument| instrument.name)
        .collect();

    for (identifier, name) in declared_metric_names() {
        assert!(
            catalogued.contains(name.as_str()),
            "{identifier} records `{name}`, which AGENT_DOMAIN_METRIC_INSTRUMENTS does not \
             document. Add it to the catalogue and to \
             docs/rakka-agent-observability-catalogue.md."
        );
    }
}

#[test]
fn every_catalogued_metric_is_a_name_the_crate_declares() {
    let declared: BTreeSet<String> = declared_metric_names()
        .into_iter()
        .map(|(_, name)| name)
        .collect();

    for instrument in AGENT_DOMAIN_METRIC_INSTRUMENTS {
        assert!(
            declared.contains(instrument.name),
            "the catalogue documents `{}`, which no METRIC_AGENT_* constant in this crate binds",
            instrument.name
        );
    }
}

#[test]
fn every_catalogued_label_key_passes_the_bounded_guard() {
    for instrument in AGENT_DOMAIN_METRIC_INSTRUMENTS {
        for key in instrument.labels {
            assert!(
                validate_agent_domain_metric_attributes(&[(key, "bounded-value")]).is_ok(),
                "`{key}` on `{}` is outside the bounded vocabulary",
                instrument.name
            );
        }
        // A label key an instrument declares must also be one the guard would
        // not reject for its *value* shape, so a bounded key cannot be paired
        // with an unbounded value at a call site and pass by accident.
        let overlong = "x".repeat(512);
        for key in instrument.labels {
            assert!(
                validate_agent_domain_metric_attributes(&[(key, overlong.as_str())]).is_err(),
                "`{key}` accepted an unbounded value"
            );
        }
    }
}

#[test]
fn the_lookup_and_the_export_views_agree_with_the_table() {
    let views = agent_domain_instrument_views();
    assert_eq!(views.len(), AGENT_DOMAIN_METRIC_INSTRUMENTS.len());
    for (instrument, view) in AGENT_DOMAIN_METRIC_INSTRUMENTS.iter().zip(views) {
        assert_eq!(view.name, instrument.name);
        assert_eq!(view.unit, instrument.unit);
        assert_eq!(view.bucket_boundaries, instrument.buckets);
        let found = agent_domain_metric_instrument(instrument.name)
            .expect("a catalogued name resolves by lookup");
        assert_eq!(found, instrument);
    }
    assert!(agent_domain_metric_instrument("rakka.agent.not.a.metric").is_none());
}

/// Every stable `error.type` the crate writes is catalogued, and every
/// catalogued one is written.
///
/// `AgentTelemetrySegment::failed` takes a `&'static str`, so the compiler
/// checks nothing about it — and [17.6](../../../docs/plans/rakka-agent/spec.md)
/// makes `error.type` a grouping attribute while 17.16 asks a retention policy
/// to select on it, so these strings are a compatibility surface an operator
/// writes Collector rules against. That is exactly the shape the metric
/// catalogue exists for, so it gets the same bijection: a new type nobody
/// documented fails here rather than shipping, and a documented type nothing
/// writes fails here rather than sitting in a doc as a promise.
#[test]
fn every_segment_error_type_the_crate_writes_is_catalogued() {
    const MARKER: &str = ".failed(";
    let mut written = BTreeSet::new();
    let entries = fs::read_dir(crate_src()).expect("the crate's src directory is readable");
    for entry in entries {
        let path = entry.expect("a directory entry is readable").path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("a source file is readable");
        for chunk in source.split(MARKER).skip(1) {
            // The first argument is the error type, and every call site in
            // this crate passes it as a literal.
            let Some((_, rest)) = chunk.split_once('"') else {
                continue;
            };
            let Some((value, _)) = rest.split_once('"') else {
                continue;
            };
            written.insert(value.to_string());
        }
    }
    assert!(
        !written.is_empty(),
        "the scan found no failed segments at all, so it is proving nothing"
    );

    let catalogued: BTreeSet<&str> = AGENT_SEGMENT_ERROR_TYPES.iter().copied().collect();
    for value in &written {
        assert!(
            catalogued.contains(value.as_str()),
            "`{value}` is written onto a failed segment but is not in \
             AGENT_SEGMENT_ERROR_TYPES, so no operator was told it exists"
        );
    }
    for value in &catalogued {
        assert!(
            written.contains(*value),
            "`{value}` is catalogued but this crate writes it nowhere"
        );
    }
}

/// Every `signal` label value this crate writes is in
/// [`AGENT_TELEMETRY_SIGNALS`], and every catalogued value is written.
///
/// The same bijection the error types get, and owed for the same reason: a
/// `signal` value is a `&'static str` the compiler checks nothing about, and
/// it is what an operator's dashboard groups telemetry loss by. Three of the
/// four instruments carrying the key were added by slice 6.3b, so without this
/// the label vocabulary would have been documentation only — which is the
/// defect class that slice was fixing.
#[test]
fn every_telemetry_signal_the_crate_writes_is_catalogued() {
    const MARKER: &str = "\"signal\", ";
    let mut written = BTreeSet::new();
    let entries = fs::read_dir(crate_src()).expect("the crate's src directory is readable");
    for entry in entries {
        let path = entry.expect("a directory entry is readable").path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("a source file is readable");
        for chunk in source.split(MARKER).skip(1) {
            // Every `("signal", …)` pair in this crate passes a literal or a
            // parameter; only the literals are this scan's business, and a
            // parameter is covered by its own caller's literal.
            let Some(rest) = chunk.strip_prefix('"') else {
                continue;
            };
            let Some((value, _)) = rest.split_once('"') else {
                continue;
            };
            written.insert(value.to_string());
        }
    }
    assert!(
        !written.is_empty(),
        "the scan found no signal labels at all, so it is proving nothing"
    );

    let catalogued: BTreeSet<&str> = AGENT_TELEMETRY_SIGNALS.iter().copied().collect();
    for value in &written {
        assert!(
            catalogued.contains(value.as_str()),
            "`{value}` is written as a `signal` label but is not in \
             AGENT_TELEMETRY_SIGNALS, so no operator was told it exists"
        );
    }
    for value in &catalogued {
        assert!(
            written.contains(*value),
            "`{value}` is catalogued as a signal but this crate writes it nowhere"
        );
    }
}
