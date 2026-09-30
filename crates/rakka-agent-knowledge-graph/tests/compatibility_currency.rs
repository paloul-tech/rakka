//! The knowledge graph's rows of `docs/rakka-compatibility.md`, held to the
//! constants and labels this crate declares.
//!
//! `rakka-agent`'s `compatibility_currency` test holds the rest of the durable
//! record table, but the crate DAG keeps it from seeing this crate, so it only
//! requires these rows to exist. The values are held here, from the same
//! `ClaimRecordKind::as_label` every schema error emits and the same constants
//! every record is written with — never from literals or from scraping source
//! text, either of which passes a rename or fails a refactor that changes no
//! persisted byte.

use std::collections::BTreeMap;

use rakka_agent_knowledge_graph::{
    ClaimRecordKind, CURRENT_CLAIM_SCHEMA_VERSION, CURRENT_CLAIM_TRUST_TRANSITION_SCHEMA_VERSION,
};

const COMPATIBILITY: &str = include_str!("../../../docs/rakka-compatibility.md");
const RECORDS_HEADING: &str = "### Durable record schema versions";
const CRATE: &str = "rakka-agent-knowledge-graph";

/// The text after a heading, up to the next heading of any level; a `#`
/// inside a fenced block is a shell comment and does not end the section.
/// (A copy of `rakka-agent`'s `tests/doc_support` reader: the crate DAG keeps
/// this crate's tests from sharing that module.)
fn section<'a>(document: &'a str, heading: &str) -> &'a str {
    let start = document
        .find(heading)
        .unwrap_or_else(|| panic!("docs/rakka-compatibility.md has no heading {heading:?}"));
    let rest = &document[start + heading.len()..];
    let mut in_fence = false;
    let mut end = 0;
    for line in rest.split_inclusive('\n') {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
        } else if !in_fence && line.starts_with('#') {
            return &rest[..end];
        }
        end += line.len();
    }
    rest
}

/// The first backticked token of one table cell.
fn backticked(cell: &str) -> Option<&str> {
    cell.split('`').nth(1)
}

/// The record table's rows naming this crate: label to version.
fn knowledge_graph_rows(section: &str) -> BTreeMap<String, u32> {
    let mut rows = BTreeMap::new();
    for line in section.lines() {
        if !line.starts_with("| `") {
            continue;
        }
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        // `| record | crate | version |` splits to an empty cell on each side.
        let [_, record, crate_name, version, _] = cells[..] else {
            panic!("a record row does not have three cells: {line:?}");
        };
        if backticked(crate_name) != Some(CRATE) {
            continue;
        }
        let label = backticked(record)
            .unwrap_or_else(|| panic!("a record row's first cell is backticked: {line:?}"));
        let version: u32 = version
            .parse()
            .unwrap_or_else(|error| panic!("record {label} has a non-numeric version: {error}"));
        assert!(
            rows.insert(label.to_string(), version).is_none(),
            "the table carries more than one row for `{label}`"
        );
    }
    rows
}

#[test]
fn every_knowledge_graph_record_schema_version_is_documented_and_current() {
    let declared: BTreeMap<String, u32> = [
        (
            ClaimRecordKind::Claim.as_label().to_string(),
            CURRENT_CLAIM_SCHEMA_VERSION.get(),
        ),
        (
            ClaimRecordKind::TrustTransition.as_label().to_string(),
            CURRENT_CLAIM_TRUST_TRANSITION_SCHEMA_VERSION.get(),
        ),
    ]
    .into_iter()
    .collect();
    let documented = knowledge_graph_rows(section(COMPATIBILITY, RECORDS_HEADING));
    assert_eq!(
        documented, declared,
        "the knowledge graph's rows of docs/rakka-compatibility.md disagree with the code"
    );
}
