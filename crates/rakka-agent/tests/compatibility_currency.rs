//! The agent-domain tables of `docs/rakka-compatibility.md`, held to the
//! code and the manifests they describe.
//!
//! The compatibility document is the one place a deployment reads the durable
//! record schema versions and the pinned dependencies from, and a version in
//! prose drifts the day someone bumps a constant. So the two tables are parsed
//! and compared in both directions: every record kind and every pin the code
//! declares must have a row with the same value, and no row may name something
//! the code no longer has.
//!
//! Gated on `otel` because the GenAI convention revision lives behind it; the
//! workspace validation runs `--all-features`, so the gate hides nothing.

#![cfg(feature = "otel")]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use rakka_agent::otel::AGENT_GENAI_CONVENTION_REVISION;
use rakka_agent::AgentRecordKind;
use rakka_agent_workflow::{
    CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION, CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION,
    CURRENT_AGENT_WORKFLOW_INDEX_SCHEMA_VERSION,
};

const COMPATIBILITY: &str = include_str!("../../../docs/rakka-compatibility.md");
const RECORDS_HEADING: &str = "### Durable record schema versions";
const PINS_HEADING: &str = "### Pinned dependencies";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rakka-agent manifest should live under crates/rakka-agent")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

/// The text between a heading and the next heading of any level.
fn section(document: &str, heading: &str) -> String {
    let (_, rest) = document
        .split_once(heading)
        .unwrap_or_else(|| panic!("docs/rakka-compatibility.md has no heading {heading:?}"));
    rest.split("\n#").next().unwrap_or(rest).to_string()
}

/// The backticked tokens of one table cell, in order.
fn backticked(cell: &str) -> Vec<String> {
    cell.split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

/// Table rows whose first cell is backticked: `(key, remaining cells)`.
fn table_rows(section: &str) -> Vec<(String, Vec<String>)> {
    let mut rows = Vec::new();
    for line in section.lines() {
        let Some(body) = line.strip_prefix("| `") else {
            continue;
        };
        let cells: Vec<String> = format!("`{body}")
            .split('|')
            .map(|cell| cell.trim().to_string())
            .collect();
        let key = backticked(&cells[0])
            .into_iter()
            .next()
            .expect("a row's first cell is backticked");
        rows.push((key, cells[1..].to_vec()));
    }
    assert!(
        !rows.is_empty(),
        "the table parsed to no rows, so nothing is checked"
    );
    rows
}

/// The lines of one `[table]` of a Cargo manifest, up to the next table.
fn manifest_section(manifest: &str, table: &str) -> String {
    let (_, rest) = manifest
        .split_once(&format!("{table}\n"))
        .unwrap_or_else(|| panic!("the manifest has no {table} table"));
    rest.lines()
        .take_while(|line| !line.starts_with('['))
        .collect::<Vec<_>>()
        .join("\n")
}

/// `dependency = "x"` or `dependency = { …, key = "x", … }` inside a manifest
/// section, read as the value of `key`.
fn manifest_value(manifest: &str, dependency: &str, key: &str) -> String {
    let line = manifest
        .lines()
        .find(|line| line.starts_with(&format!("{dependency} = ")))
        .unwrap_or_else(|| panic!("no `{dependency} = …` line in the manifest section"));
    let (_, value) = line
        .split_once(" = ")
        .expect("a dependency line has a value");
    let quoted = if value.trim_start().starts_with('{') {
        let (_, after_key) = value
            .split_once(&format!("{key} = "))
            .unwrap_or_else(|| panic!("`{dependency}` declares no `{key}`"));
        after_key
    } else {
        assert_eq!(
            key, "version",
            "a bare string dependency only carries a version"
        );
        value
    };
    quoted
        .split_once('"')
        .and_then(|(_, rest)| rest.split_once('"'))
        .map(|(inner, _)| inner.to_string())
        .unwrap_or_else(|| panic!("`{dependency}`'s `{key}` is not a quoted string"))
}

/// The one Collector image tag a topology pins, asserting every occurrence agrees.
fn collector_tag(relative: &str) -> String {
    const IMAGE: &str = "image: otel/opentelemetry-collector-contrib:";
    let tags: Vec<String> = read(relative)
        .lines()
        .filter_map(|line| line.trim().strip_prefix(IMAGE).map(str::to_string))
        .collect();
    assert!(!tags.is_empty(), "{relative} pins no Collector image");
    assert!(
        tags.iter().all(|tag| tag == &tags[0]),
        "{relative} pins more than one Collector tag: {tags:?}"
    );
    tags[0].clone()
}

/// A knowledge-graph schema constant, read from its source because the crate
/// DAG forbids `rakka-agent` from depending on the graph crate.
fn knowledge_graph_schema_version(relative: &str, constant: &str) -> u32 {
    let source = read(relative);
    let (_, rest) = source
        .split_once(&format!("pub const {constant}:"))
        .unwrap_or_else(|| panic!("{relative} declares no {constant}"));
    let statement = rest
        .split_once(';')
        .map(|(statement, _)| statement)
        .unwrap_or(rest);
    let (_, after_new) = statement
        .split_once("new(")
        .unwrap_or_else(|| panic!("{constant} is not built with `new(`"));
    after_new
        .split_once(')')
        .and_then(|(digits, _)| digits.trim().parse().ok())
        .unwrap_or_else(|| panic!("{constant} does not wrap an integer literal"))
}

/// Every schema version the code declares, keyed the way the document keys it.
fn declared_schema_versions() -> BTreeMap<String, (String, u32)> {
    let mut declared = BTreeMap::new();
    for kind in AgentRecordKind::ALL {
        declared.insert(
            kind.as_label().to_string(),
            (
                "rakka-agent".to_string(),
                kind.current_schema_version().get(),
            ),
        );
    }
    for (constant, version) in [
        (
            "CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION",
            CURRENT_AGENT_GRAPH_STATE_SCHEMA_VERSION.get(),
        ),
        (
            "CURRENT_AGENT_WORKFLOW_INDEX_SCHEMA_VERSION",
            CURRENT_AGENT_WORKFLOW_INDEX_SCHEMA_VERSION.get(),
        ),
        (
            "CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION",
            CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION.get(),
        ),
    ] {
        declared.insert(
            constant.to_string(),
            ("rakka-agent-workflow".to_string(), version),
        );
    }
    declared.insert(
        "claim".to_string(),
        (
            "rakka-agent-knowledge-graph".to_string(),
            knowledge_graph_schema_version(
                "crates/rakka-agent-knowledge-graph/src/claim.rs",
                "CURRENT_CLAIM_SCHEMA_VERSION",
            ),
        ),
    );
    declared.insert(
        "claim-trust-transition".to_string(),
        (
            "rakka-agent-knowledge-graph".to_string(),
            knowledge_graph_schema_version(
                "crates/rakka-agent-knowledge-graph/src/transition.rs",
                "CURRENT_CLAIM_TRUST_TRANSITION_SCHEMA_VERSION",
            ),
        ),
    );
    declared
}

/// Every pin the manifests and constants declare, keyed the way the document
/// keys it. The `A2A protocol` row is held by `rakka-a2a`'s own tests, which
/// can see the SDK; it is required to exist here and checked there.
fn declared_pins() -> BTreeMap<String, String> {
    let root_manifest = read("Cargo.toml");
    let workspace = manifest_section(&root_manifest, "[workspace.dependencies]");
    let agent_manifest = read("crates/rakka-agent/Cargo.toml");

    assert_eq!(manifest_value(&workspace, "a2a", "package"), "a2a-lf");
    assert_eq!(
        manifest_value(&workspace, "a2a-server", "package"),
        "a2a-server-lf"
    );
    let opentelemetry = manifest_value(&workspace, "opentelemetry", "version");
    for sibling in [
        "opentelemetry-appender-tracing",
        "opentelemetry-otlp",
        "opentelemetry_sdk",
        "opentelemetry-proto",
    ] {
        assert_eq!(
            manifest_value(&workspace, sibling, "version"),
            opentelemetry,
            "{sibling} is pinned apart from the SDK generation"
        );
    }

    BTreeMap::from([
        (
            "a2a-lf".to_string(),
            manifest_value(&workspace, "a2a", "version"),
        ),
        (
            "a2a-server-lf".to_string(),
            manifest_value(&workspace, "a2a-server", "version"),
        ),
        (
            "rig-core".to_string(),
            manifest_value(&agent_manifest, "rig-core", "version"),
        ),
        ("opentelemetry".to_string(), opentelemetry),
        (
            "GenAI semantic conventions".to_string(),
            AGENT_GENAI_CONVENTION_REVISION.to_string(),
        ),
        (
            "opentelemetry-collector-contrib (agent)".to_string(),
            collector_tag("docs/plans/rakka-agent/kubernetes-agent-otel-collector-topology.yaml"),
        ),
        (
            "opentelemetry-collector-contrib (workflow)".to_string(),
            collector_tag("docs/plans/agentic-workflow/kubernetes-otel-collector-topology.yaml"),
        ),
    ])
}

#[test]
fn every_durable_record_schema_version_is_documented_and_current() {
    let declared = declared_schema_versions();
    let documented: BTreeMap<String, (String, u32)> =
        table_rows(&section(COMPATIBILITY, RECORDS_HEADING))
            .into_iter()
            .map(|(key, cells)| {
                let crate_name = backticked(&cells[0])
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| panic!("record {key} names no crate"));
                let version = cells[1].parse::<u32>().unwrap_or_else(|error| {
                    panic!("record {key} has a non-numeric version: {error}")
                });
                (key, (crate_name, version))
            })
            .collect();

    for (key, expected) in &declared {
        let documented = documented
            .get(key)
            .unwrap_or_else(|| panic!("record {key} (version {}) has no row", expected.1));
        assert_eq!(
            documented, expected,
            "record {key} is documented as {documented:?} but the code declares {expected:?}"
        );
    }
    for key in documented.keys() {
        assert!(
            declared.contains_key(key),
            "the document lists record {key}, which the code does not declare"
        );
    }
    assert_eq!(
        documented.len(),
        AgentRecordKind::ALL.len() + 3 + 2,
        "the row count must equal every record kind the three crates declare"
    );
}

#[test]
fn every_pinned_dependency_matches_its_manifest_or_constant() {
    let declared = declared_pins();
    let documented: BTreeMap<String, String> = table_rows(&section(COMPATIBILITY, PINS_HEADING))
        .into_iter()
        .map(|(key, cells)| {
            let pin = backticked(&cells[0])
                .into_iter()
                .next()
                .unwrap_or_else(|| panic!("dependency {key} has no backticked pin"));
            (key, pin)
        })
        .collect();

    for (key, expected) in &declared {
        let documented = documented
            .get(key)
            .unwrap_or_else(|| panic!("dependency {key} (pinned {expected}) has no row"));
        assert_eq!(
            documented, expected,
            "dependency {key} is documented as {documented} but pinned at {expected}"
        );
    }
    assert!(
        documented.contains_key("A2A protocol"),
        "the A2A protocol row is missing; rakka-a2a holds its value"
    );
    for key in documented.keys() {
        assert!(
            key == "A2A protocol" || declared.contains_key(key),
            "the document pins {key}, which nothing in the tree declares"
        );
    }
}

#[test]
fn the_tests_section_names_the_commands_that_hold_the_tables() {
    let tests = section(COMPATIBILITY, "## Tests");
    for command in [
        "cargo test -p rakka-agent --features otel --test compatibility_currency",
        "cargo test -p rakka-agent --test recovery_scenario_roster",
        "cargo test -p rakka-testkit --test repository_hygiene",
    ] {
        assert!(
            tests.contains(command),
            "the Tests section does not name `{command}`"
        );
    }
}
