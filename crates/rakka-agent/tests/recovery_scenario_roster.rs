//! Specification 18's sixty-one recovery scenarios, each held to the test
//! that proves it.
//!
//! `docs/rakka-agent-recovery-scenarios.md` is a table: scenario number,
//! milestone, fidelity, proving files. A table nobody checks drifts the day a
//! test is renamed, so this suite reads it and holds it to the tree in both
//! directions — every row cites files that exist and cite that scenario, and
//! every test module that cites a scenario is in its row. The multi-pod subset
//! is cross-checked against the fault-injection matrix's authority sentence,
//! and the row count against the specification's own list.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;

mod doc_support;
use doc_support::{backticked, read, repo_root, rust_files};

const ROSTER: &str = include_str!("../../../docs/rakka-agent-recovery-scenarios.md");
const FAULT_MATRIX: &str = include_str!("../../../docs/rakka-agent-fault-injection-matrix.md");
const SPEC: &str = include_str!("../../../docs/plans/rakka-agent/spec.md");

const MULTI_POD_HARNESS: &str = "examples/multi-pod-agent-fault-soak/";
const MULTI_POD_AUTHORITY: &str =
    "Specification 18 scenarios this re-proves at multi-pod fidelity:";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fidelity {
    InProcess,
    MultiPod,
    Both,
}

impl Fidelity {
    fn parse(cell: &str) -> Self {
        match cell.trim() {
            "in-process" => Self::InProcess,
            "multi-pod" => Self::MultiPod,
            "in-process + multi-pod" => Self::Both,
            other => panic!("unknown fidelity {other:?}"),
        }
    }

    fn multi_pod(self) -> bool {
        matches!(self, Self::MultiPod | Self::Both)
    }
}

#[derive(Debug)]
struct Row {
    number: u32,
    milestone: String,
    fidelity: Fidelity,
    /// Repository-relative paths of the files that prove the row.
    proofs: Vec<String>,
}

fn roster_rows(markdown: &str) -> Vec<Row> {
    let mut rows = Vec::new();
    for line in markdown.lines() {
        let Some(body) = line.strip_prefix('|') else {
            continue;
        };
        let cells: Vec<&str> = body.split('|').collect();
        if cells.len() < 5 {
            continue;
        }
        let Ok(number) = cells[0].trim().parse::<u32>() else {
            continue;
        };
        let proofs = backticked(cells[3]);
        assert!(!proofs.is_empty(), "scenario {number} cites no proof");
        rows.push(Row {
            number,
            milestone: cells[1].trim().to_string(),
            fidelity: Fidelity::parse(cells[2]),
            proofs,
        });
    }
    assert!(
        !rows.is_empty(),
        "the roster parsed to no rows, so nothing is checked"
    );
    rows
}

/// The milestones specification 18's opening paragraph binds scenarios to,
/// read from the paragraph itself rather than copied out of it: every
/// `scenarios … bind at M<n>` clause names its scenarios, and the
/// `All other scenarios … bind at M<n>` clause supplies the default returned
/// beside the map.
fn milestone_bindings(spec: &str) -> (BTreeMap<u32, String>, String) {
    let section = spec
        .split_once("## 18. Required Recovery Scenarios")
        .map(|(_, rest)| rest)
        .expect("the specification has section 18");
    let paragraph = section
        .split("\n\n")
        .find(|paragraph| paragraph.contains("bind at M"))
        .expect("section 18 opens with the paragraph binding scenarios to milestones");
    let paragraph = paragraph.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut bound: BTreeMap<u32, String> = BTreeMap::new();
    let mut default = None;
    for clause in paragraph.split([';', '.']) {
        let Some((scenarios, rest)) = clause.split_once("bind at M") else {
            continue;
        };
        let number: String = rest.chars().take_while(char::is_ascii_digit).collect();
        assert!(
            !number.is_empty(),
            "a binding clause names no milestone number: {clause:?}"
        );
        let milestone = format!("M{number}");
        if scenarios.contains("All other scenarios") {
            assert!(
                default.replace(milestone).is_none(),
                "the paragraph has two default clauses"
            );
            continue;
        }
        let cited = cited_scenarios(scenarios);
        assert!(
            !cited.is_empty(),
            "a binding clause names no scenario: {clause:?}"
        );
        for scenario in cited {
            assert!(
                bound.insert(scenario, milestone.clone()).is_none(),
                "the paragraph binds scenario {scenario} twice"
            );
        }
    }
    assert!(
        !bound.is_empty(),
        "the paragraph binds no scenario to a later milestone"
    );
    let default = default.expect("the paragraph binds all other scenarios to a milestone");
    (bound, default)
}

/// How many numbered items specification 18 lists.
fn spec_scenario_count(spec: &str) -> usize {
    let section = spec
        .split_once("## 18. Required Recovery Scenarios")
        .map(|(_, rest)| rest)
        .expect("the specification has section 18");
    let section = section
        .split_once("\n## 19.")
        .map(|(section, _)| section)
        .expect("section 19 follows section 18");
    section
        .lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            let digits = trimmed.chars().take_while(char::is_ascii_digit).count();
            digits > 0 && trimmed[digits..].starts_with(". ")
        })
        .count()
}

/// Only the leading `//!` block of a test file, joined so a citation that
/// wraps across doc-comment lines reads as one sentence.
///
/// This is the one place a proof declares what it proves, and both directions
/// of the roster check read it — never the body, where `// unlike scenario 12`
/// in a comment would otherwise stand in for a citation.
fn module_doc(source: &str) -> String {
    source
        .lines()
        .take_while(|line| line.trim_start().starts_with("//!"))
        .map(|line| line.trim_start().trim_start_matches("//!").trim())
        .collect::<Vec<_>>()
        .join(" ")
}

fn strip_punctuation(token: &str) -> &str {
    token.trim_matches(|character: char| {
        matches!(
            character,
            ',' | ';' | ':' | '(' | ')' | '"' | '\'' | '.' | '*' | '[' | ']'
        )
    })
}

fn parse_number(token: &str) -> Option<u32> {
    let token = strip_punctuation(token);
    let token = token.strip_suffix("'s").unwrap_or(token);
    let token = strip_punctuation(token);
    if token.is_empty() || !token.chars().all(|character| character.is_ascii_digit()) {
        return None;
    }
    token.parse().ok()
}

/// A token that is a `A-B` or `A–B` range.
fn parse_range(token: &str) -> Option<(u32, u32)> {
    let token = strip_punctuation(token);
    let (start, end) = token.split_once(['-', '–'])?;
    Some((parse_number(start)?, parse_number(end)?))
}

/// Every scenario number a text cites after the word `scenario(s)`.
///
/// After the keyword the walk accepts numbers, `A-B` ranges, `/`-joined pairs,
/// and the connectors `and`, `through`, `to`, `/`, `-`, `&`; any other word
/// stops it, and so does a dotted specification reference such as `12.6`. So
/// `scenario 47 of section 18` yields {47} and `Scenario 3 / spec 12.6` yields
/// {3}.
fn cited_scenarios(text: &str) -> BTreeSet<u32> {
    let tokens: Vec<&str> = text
        .split_whitespace()
        .flat_map(|token| {
            // `Scenario 23/24` and `scenario-13` arrive as one token each.
            let lower = token.to_ascii_lowercase();
            if lower.starts_with("scenario") && token.contains('-') {
                token.splitn(2, '-').collect::<Vec<_>>()
            } else if token != "/" && token.contains('/') {
                token
                    .split('/')
                    .filter(|part| !part.is_empty())
                    .flat_map(|part| ["/", part])
                    .skip(1)
                    .collect()
            } else {
                vec![token]
            }
        })
        .collect();
    let mut cited = BTreeSet::new();
    let mut index = 0;
    while index < tokens.len() {
        let keyword = strip_punctuation(tokens[index]).to_ascii_lowercase();
        index += 1;
        if keyword != "scenario" && keyword != "scenarios" {
            continue;
        }
        // `through`/`to` ranges start at the number read immediately before
        // the connector — not at the largest number cited so far, which would
        // read `scenarios 9 and 5 through 7` as 9..=7.
        let mut last_number: Option<u32> = None;
        let mut pending_range_start: Option<u32> = None;
        while index < tokens.len() {
            let token = tokens[index];
            let bare = strip_punctuation(token);
            if bare.contains('.') && bare.chars().any(|character| character.is_ascii_digit()) {
                break; // a specification section, not a scenario
            }
            if let Some((start, end)) = parse_range(token) {
                cited.extend(start..=end);
                pending_range_start = None;
                last_number = Some(end);
            } else if let Some(number) = parse_number(token) {
                match pending_range_start.take() {
                    Some(start) => cited.extend(start..=number),
                    None => {
                        cited.insert(number);
                    }
                }
                last_number = Some(number);
            } else {
                match bare.to_ascii_lowercase().as_str() {
                    "and" | "/" | "&" | "-" | "plus" => {}
                    "through" | "to" => pending_range_start = last_number,
                    _ => break,
                }
            }
            index += 1;
        }
    }
    cited
}

/// The `**N**` numbers in the fault matrix's authority sentence.
fn fault_matrix_multi_pod_subset(matrix: &str) -> BTreeSet<u32> {
    let paragraph = matrix
        .split_once(MULTI_POD_AUTHORITY)
        .map(|(_, rest)| rest)
        .expect("the fault-injection matrix still carries its multi-pod authority sentence");
    let paragraph = paragraph.split("\n\n").next().unwrap_or(paragraph);
    let subset: BTreeSet<u32> = paragraph
        .split("**")
        .skip(1)
        .step_by(2)
        .filter_map(|bold| bold.trim().parse().ok())
        .collect();
    assert!(
        !subset.is_empty(),
        "the authority sentence names no scenario in bold"
    );
    subset
}

/// Every directory the reverse check reads: the agent-domain crates' test
/// directories, every example's sources and tests, and every directory the
/// roster itself cites — so a file the roster can reach for is a file whose
/// module doc is read back, and a proof cannot cite a scenario unrostered.
fn scanned_directories(rows: &[Row]) -> BTreeSet<String> {
    let mut directories: BTreeSet<String> = [
        "crates/rakka-agent/tests/",
        "crates/rakka-a2a/tests/",
        "crates/rakka-agent-postgres/tests/",
        "crates/rakka-agent-knowledge-graph/tests/",
        "crates/rakka-agent-knowledge-graph-postgres/tests/",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    let examples = repo_root().join("examples");
    for entry in fs::read_dir(&examples).expect("the examples directory is readable") {
        let path = entry.expect("an example entry is readable").path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        for subdirectory in ["src", "tests"] {
            if path.join(subdirectory).is_dir() {
                directories.insert(format!("examples/{name}/{subdirectory}/"));
            }
        }
    }
    for row in rows {
        for proof in &row.proofs {
            if let Some((directory, _)) = proof.rsplit_once('/') {
                directories.insert(format!("{directory}/"));
            }
        }
    }
    directories
}

#[test]
fn the_roster_numbers_every_specification_18_scenario_once() {
    let rows = roster_rows(ROSTER);
    let numbers: Vec<u32> = rows.iter().map(|row| row.number).collect();
    let listed = u32::try_from(spec_scenario_count(SPEC)).expect("a countable list");
    assert!(listed > 0, "specification 18 lists no scenarios");
    let expected: Vec<u32> = (1..=listed).collect();
    assert_eq!(
        numbers, expected,
        "the roster must list specification 18's scenarios 1..={listed} once, in order"
    );
}

#[test]
fn every_row_carries_the_milestone_the_specification_binds() {
    let (bound, default) = milestone_bindings(SPEC);
    for row in roster_rows(ROSTER) {
        let expected = bound.get(&row.number).unwrap_or(&default);
        assert_eq!(
            &row.milestone, expected,
            "scenario {} is bound to the wrong milestone",
            row.number
        );
    }
}

#[test]
fn the_milestone_paragraph_binds_every_later_milestone() {
    // A misparse that lost a clause would bind its scenarios to the default
    // and fail the roster loudly; this pins the other direction, so a clause
    // the reader cannot see at all is noticed even if no row depends on it.
    let (bound, default) = milestone_bindings(SPEC);
    assert_eq!(default, "M1");
    let milestones: BTreeSet<&str> = bound.values().map(String::as_str).collect();
    assert_eq!(milestones, BTreeSet::from(["M2", "M3", "M4", "M5"]));
}

#[test]
fn every_cited_proof_exists() {
    let root = repo_root();
    for row in roster_rows(ROSTER) {
        for proof in &row.proofs {
            assert!(
                root.join(proof).is_file(),
                "scenario {} cites {proof}, which is not a file",
                row.number
            );
        }
    }
}

#[test]
fn every_cited_file_cites_its_scenario() {
    let mut sources: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
    let mut uncited = Vec::new();
    for row in roster_rows(ROSTER) {
        for proof in &row.proofs {
            let cited = sources
                .entry(proof.clone())
                .or_insert_with(|| cited_scenarios(&module_doc(&read(proof))));
            if !cited.contains(&row.number) {
                uncited.push(format!(
                    "scenario {} <- {proof} (module doc names {cited:?})",
                    row.number
                ));
            }
        }
    }
    assert!(
        uncited.is_empty(),
        "roster rows cite files whose module docs do not name the scenario:\n{}",
        uncited.join("\n")
    );
}

#[test]
fn multi_pod_rows_cite_the_harness_and_only_they_do() {
    for row in roster_rows(ROSTER) {
        let cites_harness = row
            .proofs
            .iter()
            .any(|proof| proof.starts_with(MULTI_POD_HARNESS));
        assert_eq!(
            cites_harness,
            row.fidelity.multi_pod(),
            "scenario {} is {:?} but its harness citation says otherwise",
            row.number,
            row.fidelity
        );
    }
}

#[test]
fn the_fault_matrix_and_the_roster_agree_on_the_multi_pod_subset() {
    let rostered: BTreeSet<u32> = roster_rows(ROSTER)
        .into_iter()
        .filter(|row| row.fidelity.multi_pod())
        .map(|row| row.number)
        .collect();
    assert_eq!(
        rostered,
        fault_matrix_multi_pod_subset(FAULT_MATRIX),
        "the roster's multi-pod rows and the fault matrix's authority sentence disagree"
    );
}

#[test]
fn every_module_doc_citation_is_rostered() {
    let rows = roster_rows(ROSTER);
    let mut rostered_files: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();
    for row in &rows {
        rostered_files
            .entry(row.number)
            .or_default()
            .extend(row.proofs.iter().cloned());
    }
    let mut checked = 0;
    let mut unrostered = Vec::new();
    for dir in scanned_directories(&rows) {
        for (relative, source) in rust_files(&dir) {
            for scenario in cited_scenarios(&module_doc(&source)) {
                checked += 1;
                let rostered = rostered_files
                    .get(&scenario)
                    .is_some_and(|files| files.contains(&relative));
                if !rostered {
                    unrostered.push(format!("scenario {scenario} <- {relative}"));
                }
            }
        }
    }
    assert!(
        checked > 0,
        "no module doc cited any scenario, so nothing was checked"
    );
    assert!(
        unrostered.is_empty(),
        "module docs cite scenarios whose roster rows do not cite them:\n{}",
        unrostered.join("\n")
    );
}

#[test]
fn the_citation_reader_handles_every_shape_the_suites_use() {
    let cases: &[(&str, &[u32])] = &[
        ("scenario 27's quorum", &[27]),
        ("Scenario 54 / 44 of section 18", &[54, 44]),
        ("Scenarios 5-10 and the", &[5, 6, 7, 8, 9, 10]),
        ("scenarios 5 through 9 as well", &[5, 6, 7, 8, 9]),
        ("scenarios 9 and 5 through 7", &[9, 5, 6, 7]),
        ("scenarios 1-3 to 5", &[1, 2, 3, 4, 5]),
        (
            "Scenarios 3, 11, 12, and the reconciliation half of 57",
            &[3, 11, 12],
        ),
        ("Scenario 23/24 links", &[23, 24]),
        ("scenario 42 hardening", &[42]),
        (
            "scenario 47 of section 18, plus the duplicate-scan half of scenario 48",
            &[47, 48],
        ),
        ("Scenario 3 / spec 12.6", &[3]),
        ("scenarios 16 and 18 are the clauses", &[16, 18]),
        ("the scenario-13 proof", &[13]),
        (
            "scenario 46 of section 18, which is open decision 20's proof",
            &[46],
        ),
        ("no citation here", &[]),
    ];
    for (text, expected) in cases {
        let expected: BTreeSet<u32> = expected.iter().copied().collect();
        assert_eq!(cited_scenarios(text), expected, "reading {text:?}");
    }
}
