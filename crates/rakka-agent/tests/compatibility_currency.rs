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
///
/// A key may appear once. The callers collect rows into a map, which would
/// keep the last row for a key and silently pass a table carrying a stale row
/// above a current one — the shape a version bump or a merge leaves behind —
/// so duplicates fail here, where every row is still visible.
fn table_rows(section: &str) -> Vec<(String, Vec<String>)> {
    let mut rows: Vec<(String, Vec<String>)> = Vec::new();
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
        assert!(
            rows.iter().all(|(held, _)| held != &key),
            "the table carries more than one row for `{key}`"
        );
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
/// section, read as the value of `key`. A quoted value is returned without its
/// quotes; a bare one (`default-features = false`) as written.
fn manifest_value(manifest: &str, dependency: &str, key: &str) -> String {
    let line = manifest
        .lines()
        .find(|line| line.starts_with(&format!("{dependency} = ")))
        .unwrap_or_else(|| panic!("no `{dependency} = …` line in the manifest section"));
    let (_, value) = line
        .split_once(" = ")
        .expect("a dependency line has a value");
    let raw = if value.trim_start().starts_with('{') {
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
    let raw = raw.trim_start();
    if let Some(rest) = raw.strip_prefix('"') {
        return rest
            .split_once('"')
            .map(|(inner, _)| inner.to_string())
            .unwrap_or_else(|| panic!("`{dependency}`'s `{key}` has an unterminated string"));
    }
    let bare = raw
        .split([',', '}'])
        .next()
        .map(str::trim)
        .filter(|bare| !bare.is_empty())
        .unwrap_or_else(|| panic!("`{dependency}`'s `{key}` has no value"));
    bare.to_string()
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

/// The crate whose rows this test cannot see: the DAG forbids `rakka-agent`
/// from depending on the graph crate, so `rakka-agent-knowledge-graph`'s own
/// `compatibility_currency` test holds its rows to its constants and labels.
/// Here they are required to exist and to belong to that crate, nothing more.
const KNOWLEDGE_GRAPH_CRATE: &str = "rakka-agent-knowledge-graph";

/// Every schema version this test can see the code declare, keyed the way the
/// document keys it.
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
    // The `a2a-server-lf` row promises that no TLS provider is forced on
    // applications; that promise is the one `default-features = false` on
    // the workspace line, which every member inherits, so it is held here.
    assert_eq!(
        manifest_value(&workspace, "a2a-server", "default-features"),
        "false",
        "a2a-server must be imported without default features"
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
    let mut knowledge_graph_rows = 0;
    for (key, (crate_name, _)) in &documented {
        if crate_name == KNOWLEDGE_GRAPH_CRATE {
            knowledge_graph_rows += 1;
            continue;
        }
        assert!(
            declared.contains_key(key),
            "the document lists record {key}, which the code does not declare"
        );
    }
    assert!(
        knowledge_graph_rows > 0,
        "the knowledge graph's rows are missing; its own compatibility_currency test holds their values"
    );
    assert_eq!(
        documented.len() - knowledge_graph_rows,
        AgentRecordKind::ALL.len() + 3,
        "the row count must equal every record kind the two crates declare"
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

/// The telemetry validation matrix repeats four pins beside its rationale. A
/// hand-copied pin is exactly what drifts when the compatibility table moves,
/// so each row is held to the pin the manifests and constants declare.
#[test]
fn the_telemetry_matrix_repeats_the_pins_the_compatibility_table_holds() {
    const MATRIX: &str = include_str!("../../../docs/rakka-agent-telemetry-validation-matrix.md");
    let declared = declared_pins();
    let rows: BTreeMap<String, Vec<String>> = section(MATRIX, "## Pinned versions")
        .lines()
        .filter(|line| line.starts_with("| ") && !line.starts_with("| ---"))
        .skip(1)
        .map(|line| {
            let cells: Vec<&str> = line.split('|').map(str::trim).collect();
            assert!(
                cells.len() >= 4,
                "a pinned-versions row has fewer than three cells: {line:?}"
            );
            (cells[1].to_string(), backticked(cells[2]))
        })
        .collect();
    type PinReader = fn(&str) -> String;
    let expectations: [(&str, &str, PinReader); 4] = [
        ("OpenTelemetry Rust SDK", "opentelemetry", |token| {
            token.to_string()
        }),
        (
            "Collector distribution (agent domain)",
            "opentelemetry-collector-contrib (agent)",
            |token| {
                token
                    .rsplit_once(':')
                    .map(|(_, tag)| tag.to_string())
                    .unwrap_or_default()
            },
        ),
        (
            "Collector distribution (workflow domain)",
            "opentelemetry-collector-contrib (workflow)",
            |token| {
                token
                    .rsplit_once(':')
                    .map(|(_, tag)| tag.to_string())
                    .unwrap_or_default()
            },
        ),
        (
            "GenAI semantic conventions",
            "GenAI semantic conventions",
            |token| token.to_string(),
        ),
    ];
    assert_eq!(
        rows.len(),
        expectations.len(),
        "the matrix's pinned-versions table gained or lost a row: {:?}",
        rows.keys().collect::<Vec<_>>()
    );
    for (component, pin, read) in expectations {
        let tokens = rows
            .get(component)
            .unwrap_or_else(|| panic!("the matrix has no pinned-versions row for {component}"));
        let documented = tokens
            .last()
            .map(|token| read(token))
            .unwrap_or_else(|| panic!("the {component} row has no backticked pin"));
        assert_eq!(
            &documented, &declared[pin],
            "the matrix pins {component} at {documented}, but the tree declares {}",
            declared[pin]
        );
    }
}

#[test]
fn the_tests_section_names_the_commands_that_hold_the_tables() {
    let tests = section(COMPATIBILITY, "## Tests");
    for command in [
        "cargo test -p rakka-agent --features otel --test compatibility_currency",
        "cargo test -p rakka-agent-knowledge-graph --test compatibility_currency",
        "cargo test -p rakka-agent --test product_doc_currency",
        "cargo test -p rakka-agent --test recovery_scenario_roster",
        "cargo test -p rakka-testkit --test repository_hygiene",
    ] {
        assert!(
            tests.contains(command),
            "the Tests section does not name `{command}`"
        );
    }
}
