//! The product documents' claims about the exchange vocabulary, held to the
//! code.
//!
//! `docs/rakka-agents.md` and `docs/rakka-v1-reliability-boundaries.md` each
//! list every inter-entity exchange kind, and the reliability document attaches
//! a delivery guarantee to the list. A list in prose drifts the day a slice adds
//! an exchange, and a stale one misstates what the courier re-drives — the
//! first version of both lists named delegation and handoff sends, which are
//! outbox effects, and omitted two real kinds. So both lists are parsed and
//! compared to `AgentExchangeKind::ALL`, label for label.

use std::collections::BTreeSet;

use rakka_agent::AgentExchangeKind;

const AGENTS: &str = include_str!("../../../docs/rakka-agents.md");
const RELIABILITY: &str = include_str!("../../../docs/rakka-v1-reliability-boundaries.md");

/// The backticked tokens of one passage, in order.
fn backticked(passage: &str) -> Vec<&str> {
    passage.split('`').skip(1).step_by(2).collect()
}

/// The paragraph (up to a blank line) that begins at the first occurrence of
/// `opening`.
fn paragraph_from<'a>(document: &'a str, opening: &str) -> &'a str {
    let (_, rest) = document
        .split_once(opening)
        .unwrap_or_else(|| panic!("the document no longer says {opening:?}"));
    rest.split("\n\n").next().unwrap_or(rest)
}

/// The labels a passage names before its source citation: both documents close
/// their list with `AgentExchangeKind::ALL`, the first backticked Rust path.
fn listed_labels(passage: &str) -> BTreeSet<String> {
    backticked(passage)
        .into_iter()
        .take_while(|token| !token.contains("::"))
        .map(str::to_string)
        .collect()
}

fn declared_labels() -> BTreeSet<String> {
    AgentExchangeKind::ALL
        .iter()
        .map(|kind| kind.as_label().to_string())
        .collect()
}

#[test]
fn the_product_document_lists_every_exchange_kind_and_nothing_else() {
    let passage = paragraph_from(
        AGENTS,
        "Entities talk to each other only through **exchanges**",
    );
    assert!(
        passage.contains("eighteen kinds") == (AgentExchangeKind::ALL.len() == 18),
        "the spelled-out count no longer matches AgentExchangeKind::ALL"
    );
    assert_eq!(
        listed_labels(passage),
        declared_labels(),
        "docs/rakka-agents.md's exchange list disagrees with AgentExchangeKind::ALL"
    );
}

#[test]
fn the_reliability_document_lists_every_exchange_kind_and_nothing_else() {
    let passage = paragraph_from(RELIABILITY, "- Every cross-entity exchange (");
    assert_eq!(
        listed_labels(passage),
        declared_labels(),
        "docs/rakka-v1-reliability-boundaries.md's exchange list disagrees with AgentExchangeKind::ALL"
    );
}
