//! The run entity hands its own delegation envelope to context assembly.
//!
//! Issue #69, [specification 13.4 and 16](../../../docs/plans/rakka-agent/spec.md).
//! What a run may read communally is its agent's grant narrowed by the
//! envelope the run accepted its assignment under. Assembly applies the
//! narrowing; this file proves the run entity is what supplies the envelope,
//! through the real settle pass, so that a run which was delegated less than
//! its agent holds can never search under the agent's whole grant.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentCommunalClaimAuthority, AgentCommunalClaimId, AgentCommunalClaimRetriever,
    AgentCommunalReadGrant, AgentCommunalReadGrantSource, AgentCommunalRetrieval,
    AgentGuardrailChain, AgentModelTurn, AgentRevisionNumber, AgentRunMemory, AgentRunScope,
    AgentTaskContent, CommunalClaimRecord, CommunalRetrievalOutcome, CommunalRetrievalPolicy,
    CommunalRetrievalQuery, InMemoryContextSnapshotStore, InMemorySessionMemoryStore,
    KnowledgeSpaceId, MemoryFuture, TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentTimestampMillis;

mod common;

use common::*;

const GOAL_SPACE: &str = "space-alpha";
const OTHER_SPACE: &str = "space-beta";

fn space(id: &str) -> KnowledgeSpaceId {
    KnowledgeSpaceId::new(id).expect("the space id is valid")
}

/// Records every space it is asked to rank, and ranks nothing.
#[derive(Default)]
struct AskedRetriever {
    asked: Mutex<Vec<String>>,
}

impl AskedRetriever {
    fn asked(&self) -> Vec<String> {
        self.asked
            .lock()
            .expect("the double's lock is never poisoned")
            .clone()
    }
}

impl AgentCommunalClaimRetriever for AskedRetriever {
    fn backend_name(&self) -> &'static str {
        "asked-double"
    }

    fn retriever_version(&self) -> AgentRevisionNumber {
        AgentRevisionNumber::INITIAL
    }

    fn retrieve<'a>(
        &'a self,
        _tenant: &'a TenantId,
        space: &'a KnowledgeSpaceId,
        _query: &'a CommunalRetrievalQuery,
        _now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, CommunalRetrievalOutcome> {
        Box::pin(async move {
            self.asked
                .lock()
                .expect("the double's lock is never poisoned")
                .push(space.as_str().to_string());
            Ok(CommunalRetrievalOutcome::default())
        })
    }
}

/// Holds nothing.
struct EmptyAuthority;

impl AgentCommunalClaimAuthority for EmptyAuthority {
    fn backend_name(&self) -> &'static str {
        "empty-double"
    }

    fn resolve<'a>(
        &'a self,
        _tenant: &'a TenantId,
        _space: &'a KnowledgeSpaceId,
        _claim: &'a AgentCommunalClaimId,
        _now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<CommunalClaimRecord>> {
        Box::pin(async move { Ok(None) })
    }
}

/// The agent's own grant: both spaces, whatever run asks.
struct BothSpaces;

impl AgentCommunalReadGrantSource for BothSpaces {
    fn backend_name(&self) -> &'static str {
        "both-spaces-double"
    }

    fn read_grant<'a>(
        &'a self,
        _scope: &'a AgentRunScope,
        _now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, Option<AgentCommunalReadGrant>> {
        Box::pin(async move {
            Ok(Some(AgentCommunalReadGrant {
                spaces: BTreeSet::from([space(GOAL_SPACE), space(OTHER_SPACE)]),
                read_capabilities: BTreeSet::new(),
                definition_revision: AgentRevisionNumber::INITIAL,
                setup_revision: None,
            }))
        })
    }
}

/// A fixture whose runs carry a communal bundle over `retriever`, with a
/// model that proposes on its first turn.
fn wired_fixture(retriever: Arc<AskedRetriever>) -> Fixture {
    let memory = AgentRunMemory::new(
        Arc::new(InMemorySessionMemoryStore::new()),
        Arc::new(InMemoryContextSnapshotStore::new()),
    )
    .with_communal_retrieval(AgentCommunalRetrieval::new(
        retriever,
        Arc::new(EmptyAuthority),
        Arc::new(BothSpaces),
        AgentGuardrailChain::new(AgentRevisionNumber::INITIAL),
        CommunalRetrievalPolicy::default(),
    ));
    Fixture::new(ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new().with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_text("I have an answer.")
                .with_proposal(
                    AgentTaskContent::inline(serde_json::json!({ "answer": "resolved" }))
                        .expect("the proposal is inline-bounded"),
                ),
        ),
    ))
    .with_memory(memory)
}

/// A goal root whose goal names one of the two spaces its agent is granted
/// searches that space and never the other: the run's envelope, not only the
/// agent's grant, decides what the turn may read.
#[tokio::test]
async fn a_run_searches_only_the_spaces_its_envelope_leaves_it() {
    let retriever = Arc::new(AskedRetriever::default());
    let fx = wired_fixture(retriever.clone());
    fx.instantiate_agent().await;
    let mut spec = goal_spec();
    spec.knowledge_spaces.insert(space(GOAL_SPACE));
    fx.apply_task_command(goal_task_creation_command(
        task_definition(),
        goal_spec_draft(spec, true),
    ))
    .await
    .expect("the goal task creates");
    // The goal has no evaluator wired here, so whether the run ends is not
    // this test's concern. Its first turn's snapshot is.
    let _ = fx.pump().await;

    let asked = retriever.asked();
    assert!(
        !asked.is_empty(),
        "the turn's assembly searched the communal tier"
    );
    assert!(
        asked.iter().all(|space| space == GOAL_SPACE),
        "a run whose envelope names one space searched another: {asked:?}"
    );
}

/// The control: a plain task's run carries no envelope, so the same agent's
/// whole grant is searched. Without this the test above could pass because
/// nothing was ever asked for the second space.
#[tokio::test]
async fn a_run_with_no_envelope_searches_its_agents_whole_grant() {
    let retriever = Arc::new(AskedRetriever::default());
    let fx = wired_fixture(retriever.clone());
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should run to completion");

    let asked: BTreeSet<String> = retriever.asked().into_iter().collect();
    assert_eq!(
        asked,
        BTreeSet::from([GOAL_SPACE.to_string(), OTHER_SPACE.to_string()])
    );
}
