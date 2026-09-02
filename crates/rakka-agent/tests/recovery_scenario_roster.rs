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
use std::path::{Path, PathBuf};

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
struct Proof {
    path: String,
    test_fn: Option<String>,
}

#[derive(Debug)]
struct Row {
    number: u32,
    milestone: String,
    fidelity: Fidelity,
    proofs: Vec<Proof>,
}

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

/// The backticked tokens of one table cell, in order.
fn backticked(cell: &str) -> Vec<String> {
    cell.split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
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
        let proofs = backticked(cells[3])
            .into_iter()
            .map(|token| match token.split_once("::") {
                Some((path, test_fn)) => Proof {
                    path: path.to_string(),
                    test_fn: Some(test_fn.to_string()),
                },
                None => Proof {
                    path: token,
                    test_fn: None,
                },
            })
            .collect::<Vec<_>>();
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

/// The milestone specification 18's opening paragraph binds a scenario to.
fn milestone_for(scenario: u32) -> &'static str {
    match scenario {
        15 | 16 | 18 | 20 => "M2",
        36 | 47..=51 => "M3",
        27..=34 | 39 => "M4",
        38 | 41..=43 | 45 => "M5",
        _ => "M1",
    }
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

/// Source text with comment markers stripped and lines joined, so a citation
/// that wraps across doc-comment lines reads as one sentence.
fn normalise(source: &str) -> String {
    source
        .lines()
        .map(|line| {
            let trimmed = line.trim_start();
            trimmed
                .strip_prefix("//!")
                .or_else(|| trimmed.strip_prefix("///"))
                .or_else(|| trimmed.strip_prefix("//"))
                .unwrap_or(trimmed)
                .trim()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Only the leading `//!` block of a test file.
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
            } else if token != "/" && token.contains('/') && parse_range(token).is_none() {
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
            } else if let Some(number) = parse_number(token) {
                match pending_range_start.take() {
                    Some(start) => cited.extend(start..=number),
                    None => {
                        cited.insert(number);
                    }
                }
            } else {
                match bare.to_ascii_lowercase().as_str() {
                    "and" | "/" | "&" | "-" | "plus" => {}
                    "through" | "to" => {
                        pending_range_start = cited.iter().next_back().copied();
                    }
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

fn test_files(relative_dir: &str) -> Vec<(String, String)> {
    let dir = repo_root().join(relative_dir);
    let mut files = Vec::new();
    for entry in fs::read_dir(&dir).unwrap_or_else(|error| panic!("read {relative_dir}: {error}")) {
        let path = entry.expect("a directory entry is readable").path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("a test file has a name");
        let relative = format!("{relative_dir}{name}");
        files.push((relative.clone(), read(&relative)));
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

#[test]
fn the_roster_numbers_every_specification_18_scenario_once() {
    let rows = roster_rows(ROSTER);
    let numbers: Vec<u32> = rows.iter().map(|row| row.number).collect();
    let expected: Vec<u32> = (1..=61).collect();
    assert_eq!(
        numbers, expected,
        "the roster must list scenarios 1..=61 once, in order"
    );
    assert_eq!(
        spec_scenario_count(SPEC),
        expected.len(),
        "specification 18 lists a different number of scenarios than the roster"
    );
}

#[test]
fn every_row_carries_the_milestone_the_specification_binds() {
    for row in roster_rows(ROSTER) {
        assert_eq!(
            row.milestone,
            milestone_for(row.number),
            "scenario {} is bound to the wrong milestone",
            row.number
        );
    }
}

#[test]
fn every_cited_proof_exists() {
    let root = repo_root();
    for row in roster_rows(ROSTER) {
        for proof in &row.proofs {
            let path = root.join(&proof.path);
            assert!(
                path.is_file(),
                "scenario {} cites {}, which is not a file",
                row.number,
                proof.path
            );
            if let Some(test_fn) = &proof.test_fn {
                let source = read(&proof.path);
                assert!(
                    source.contains(&format!("fn {test_fn}(")),
                    "scenario {} cites {}::{test_fn}, which that file does not define",
                    row.number,
                    proof.path
                );
            }
        }
    }
}

#[test]
fn every_cited_file_cites_its_scenario() {
    let mut sources: BTreeMap<String, BTreeSet<u32>> = BTreeMap::new();
    for row in roster_rows(ROSTER) {
        for proof in &row.proofs {
            let cited = sources
                .entry(proof.path.clone())
                .or_insert_with(|| cited_scenarios(&normalise(&read(&proof.path))));
            assert!(
                cited.contains(&row.number),
                "scenario {} cites {}, whose text names scenarios {:?} and not {}",
                row.number,
                proof.path,
                cited,
                row.number
            );
        }
    }
}

#[test]
fn multi_pod_rows_cite_the_harness_and_only_they_do() {
    for row in roster_rows(ROSTER) {
        let cites_harness = row
            .proofs
            .iter()
            .any(|proof| proof.path.starts_with(MULTI_POD_HARNESS));
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
            .extend(row.proofs.iter().map(|proof| proof.path.clone()));
    }
    let mut checked = 0;
    let mut unrostered = Vec::new();
    for dir in ["crates/rakka-agent/tests/", "crates/rakka-a2a/tests/"] {
        for (relative, source) in test_files(dir) {
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
