//! The decision ledger: the answer a decision's replay gets once its
//! operation id has left the entity's operation window.
//!
//! A run and a task each remember their last 64 operation ids, and a re-post
//! inside that window is answered `Duplicate`. Ordinary traffic fills the
//! window — every effect result is one entry — so a client that lost the
//! answer to a committed decision and re-posts it later used to meet the
//! refusal a first attempt would meet, and could not tell "it applied" from
//! "it was refused". The ledger is a second, smaller log that only decisions
//! write: a checkpoint resolution, a task's creation or cancellation, a goal
//! or wake-lifecycle transition. The entity consults it right after its
//! window misses and before every other guard, so a decision's replay
//! converges on its original outcome after any number of non-decision
//! operations, and up to 31 later decisions.

use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

use crate::identity::AgentOperationId;

/// How many decisions a run or a task remembers past its operation window.
pub const AGENT_DECISION_LEDGER_CAPACITY: usize = 32;

/// Bounded log of the decisions an entity committed, and the outcome each
/// produced, oldest first.
///
/// It encodes exactly as an entity's operation log does — a JSON array of
/// `{"operation_id", "outcome"}` — and an entity omits it while it is empty.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentDecisionLedger<O> {
    entries: VecDeque<AgentDecisionLedgerEntry<O>>,
}

impl<O> Default for AgentDecisionLedger<O> {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }
}

impl<O> AgentDecisionLedger<O> {
    /// The outcome the decision committed under `operation_id` produced, while
    /// the ledger still holds it.
    #[must_use]
    pub fn outcome(&self, operation_id: &AgentOperationId) -> Option<&O> {
        self.entries
            .iter()
            .find(|entry| &entry.operation_id == operation_id)
            .map(|entry| &entry.outcome)
    }

    /// How many decisions are remembered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no decision is remembered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remembers one committed decision, evicting the oldest past
    /// [`AGENT_DECISION_LEDGER_CAPACITY`].
    pub(crate) fn record(&mut self, operation_id: AgentOperationId, outcome: O) {
        self.entries.push_back(AgentDecisionLedgerEntry {
            operation_id,
            outcome,
        });
        while self.entries.len() > AGENT_DECISION_LEDGER_CAPACITY {
            self.entries.pop_front();
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AgentDecisionLedgerEntry<O> {
    operation_id: AgentOperationId,
    outcome: O,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::AgentOperationKind;

    fn id(discriminator: &str) -> AgentOperationId {
        AgentOperationId::new(
            AgentOperationKind::Command,
            ["acme", "ledger", discriminator],
        )
        .expect("the operation id derives")
    }

    #[test]
    fn a_recorded_decision_answers_its_outcome_and_an_unknown_one_answers_nothing() {
        let mut ledger = AgentDecisionLedger::default();
        ledger.record(id("approve"), 7_u32);
        assert_eq!(ledger.outcome(&id("approve")), Some(&7));
        assert_eq!(ledger.outcome(&id("deny")), None);
        assert_eq!(ledger.len(), 1);
        assert!(!ledger.is_empty());
    }

    #[test]
    fn the_ledger_keeps_the_newest_decisions_up_to_its_capacity() {
        let mut ledger = AgentDecisionLedger::default();
        for index in 0..=AGENT_DECISION_LEDGER_CAPACITY {
            ledger.record(id(&index.to_string()), index);
        }
        assert_eq!(ledger.len(), AGENT_DECISION_LEDGER_CAPACITY);
        assert_eq!(
            ledger.outcome(&id("0")),
            None,
            "the oldest decision is evicted"
        );
        assert_eq!(ledger.outcome(&id("1")), Some(&1));
        let newest = AGENT_DECISION_LEDGER_CAPACITY;
        assert_eq!(ledger.outcome(&id(&newest.to_string())), Some(&newest));
    }

    #[test]
    fn the_ledger_encodes_as_an_operation_log_does() {
        let mut ledger = AgentDecisionLedger::default();
        ledger.record(id("approve"), 7_u32);
        let encoded = serde_json::to_value(&ledger).expect("the ledger encodes");
        assert_eq!(
            encoded,
            serde_json::json!([{
                "operation_id": serde_json::to_value(id("approve")).expect("the id encodes"),
                "outcome": 7,
            }])
        );
        let decoded: AgentDecisionLedger<u32> =
            serde_json::from_value(encoded).expect("the ledger decodes");
        assert_eq!(decoded, ledger);
        assert_eq!(
            serde_json::to_value(AgentDecisionLedger::<u32>::default()).expect("encodes"),
            serde_json::json!([])
        );
    }
}
