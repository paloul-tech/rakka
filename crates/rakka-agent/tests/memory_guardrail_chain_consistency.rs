//! One declared chain, two enforcement points, and a check that they match.
//!
//! Specification: section 16 (versioned ordered guardrail stages; a
//! deployment-mandatory stage a definition cannot weaken); scenario 44.
//!
//! Guardrails are enforced at two structurally separate places. The dispatch
//! authority evaluates the model-request and tool-request boundaries before
//! every attempt's durable `Started`. The retrieval bundle evaluates the
//! memory-ingress boundary during snapshot assembly. Neither can see the
//! other's chain, and the docs asked a deployment to wire the same one into
//! both.
//!
//! Nothing checked that it had. `AGENT_EVALUATED_GUARDRAIL_BOUNDARIES`
//! unconditionally claimed the memory-ingress boundary was evaluated, so
//! `validate_covers` admitted a mandatory memory-ingress-only stage at an
//! authority that had never been shown a retrieval bundle — and a deployment
//! whose bundle carried a drifted revision, or an empty chain at the *same*
//! revision, passed every check while running no ingress stage at all.
//!
//! Why `memory_ingress_guardrails.rs::a_memory_ingress_only_mandatory_stage_now_satisfies_coverage`
//! was not enough: it proved the *constant* carried the boundary, which is
//! precisely the fail-open. Coverage now comes from an attestation the
//! authority checks, and that test moved here to require one.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedPrivateMemoryRetriever};
use rakka_agent::{
    AgentEffectSpec, AgentGuardrail, AgentGuardrailBoundary, AgentGuardrailChain,
    AgentGuardrailContext, AgentGuardrailOutcome, AgentGuardrailStage, AgentGuardrailStageId,
    AgentMemoryRetrieval, AgentModelTurn, AgentPrivateMemory, AgentPrivateMemoryId,
    AgentPrivateMemoryKind, AgentPrivateMemoryStore, AgentRevisionNumber, AgentRunMemory,
    AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentPrivateMemoryStore, InMemoryContextSnapshotStore, InMemoryPrivateMemoryRetriever,
    InMemorySessionMemoryStore, MemoryClassification, MemoryOperationId, MemoryRetrievalOutcome,
    PrivateMemoryExpectation, RetrievedPrivateMemory,
    AGENT_AUTHORITY_EVALUATED_GUARDRAIL_BOUNDARIES, AGENT_EVALUATED_GUARDRAIL_BOUNDARIES,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentTimestampMillis;

mod common;

use common::*;

const TOOL: &str = "charge-card";
const STAGE: &str = "memory-pii-filter";

fn stage_id(id: &str) -> AgentGuardrailStageId {
    AgentGuardrailStageId::new(id).expect("the stage id is valid")
}

struct AllowAll;
impl AgentGuardrail for AllowAll {
    fn evaluate(
        &self,
        _context: &AgentGuardrailContext<'_>,
        _content: &serde_json::Value,
    ) -> AgentGuardrailOutcome {
        AgentGuardrailOutcome::Allow
    }
}

/// The deployment's chain: one mandatory stage, bound only to the
/// memory-ingress boundary.
fn deployment_chain(revision: u64) -> AgentGuardrailChain {
    AgentGuardrailChain::new(AgentRevisionNumber::new(revision))
        .with_stage(
            AgentGuardrailStage::new(
                stage_id(STAGE),
                AgentRevisionNumber::INITIAL,
                Arc::new(AllowAll),
            )
            .at_boundary(AgentGuardrailBoundary::MemoryIngress)
            .mandatory(),
        )
        .expect("the stage registers")
}

/// A run memory whose retrieval bundle carries the given chain.
///
/// The helper yields the *memory*, not a bare bundle, because that is what an
/// authority now attests: a bundle handed in for the occasion proves only that
/// a matching chain exists somewhere, never that the run will evaluate it.
fn memory_with(chain: AgentGuardrailChain) -> AgentRunMemory {
    memory_over(Arc::new(InMemoryAgentPrivateMemoryStore::new()), chain)
}

/// The same, over a caller-supplied private store, so a test can seed a
/// memory the ingress chain will actually be handed.
fn memory_over(
    store: Arc<InMemoryAgentPrivateMemoryStore>,
    chain: AgentGuardrailChain,
) -> AgentRunMemory {
    AgentRunMemory::new(
        Arc::new(InMemorySessionMemoryStore::new()),
        Arc::new(InMemoryContextSnapshotStore::new()),
    )
    .with_retrieval(AgentMemoryRetrieval::new(
        Arc::new(InMemoryPrivateMemoryRetriever::new(store.clone())),
        store,
        chain,
    ))
}

/// A run memory with no retrieval bundle at all — the deployment that wires
/// session memory and nothing else.
fn memory_without_retrieval() -> AgentRunMemory {
    AgentRunMemory::new(
        Arc::new(InMemorySessionMemoryStore::new()),
        Arc::new(InMemoryContextSnapshotStore::new()),
    )
}

fn tool_id() -> AgentToolId {
    AgentToolId::new(TOOL).expect("the tool id is valid")
}

fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me do that.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("the call id is valid"),
                tool_id(),
                serde_json::json!({ "amount": 42 }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                .expect("the proposal is inline-bounded"),
        )
}

// ---------------------------------------------------------------------------
// The attestation, and what it refuses.
// ---------------------------------------------------------------------------

/// A bundle carrying the same declared chain satisfies memory-ingress
/// coverage — the flip that used to be unconditional.
#[test]
fn an_attested_bundle_carrying_the_same_chain_satisfies_memory_ingress_coverage() {
    let chain = deployment_chain(7);
    let authority = AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_guardrails(chain.clone())
    .with_memory_ingress(&memory_with(chain))
    .expect("the bundle carries the same declared chain");

    assert!(
        authority
            .evaluated_boundaries()
            .contains(&AgentGuardrailBoundary::MemoryIngress),
        "an attested authority evaluates the memory-ingress boundary"
    );
    assert_eq!(
        authority.evaluated_boundaries(),
        AGENT_EVALUATED_GUARDRAIL_BOUNDARIES,
    );
}

/// An unattested authority does *not* count the boundary, so a mandatory
/// memory-ingress-only stage would protect nothing and is refused.
#[test]
fn an_unattested_authority_does_not_count_the_memory_ingress_boundary() {
    let authority = AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_guardrails(deployment_chain(7));

    assert_eq!(
        authority.evaluated_boundaries(),
        AGENT_AUTHORITY_EVALUATED_GUARDRAIL_BOUNDARIES,
        "an authority that was never shown a bundle cannot vouch for one"
    );
    assert!(!authority
        .evaluated_boundaries()
        .contains(&AgentGuardrailBoundary::MemoryIngress));
}

/// A bundle at a different chain revision is refused at wiring time.
#[test]
fn a_bundle_at_a_different_revision_is_refused_at_wiring() {
    let error = AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_guardrails(deployment_chain(7))
    .with_memory_ingress(&memory_with(deployment_chain(8)))
    .expect_err("a drifted revision is a different declared evaluation");

    assert_eq!(error.code(), "guardrail-chain-mismatch");
}

/// The case a revision comparison alone would wave through: an *empty* bundle
/// chain carrying the same revision number.
///
/// This is the shape a deployment actually lands in — someone constructs
/// `AgentGuardrailChain::new(rev)` for the bundle and forgets to add the
/// stages — and it is why the check compares a declaration digest rather than
/// a revision.
#[test]
fn an_empty_bundle_chain_at_the_same_revision_is_refused_at_wiring() {
    let empty = AgentGuardrailChain::new(AgentRevisionNumber::new(7));
    assert_eq!(
        empty.revision(),
        deployment_chain(7).revision(),
        "the two chains agree on the revision, which is the whole point"
    );

    let error = AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_guardrails(deployment_chain(7))
    .with_memory_ingress(&memory_with(empty))
    .expect_err("an empty bundle chain runs no stage, whatever it calls itself");

    assert_eq!(error.code(), "guardrail-chain-mismatch");
}

/// An authority with no chain at all cannot attest.
#[test]
fn an_authority_with_no_chain_cannot_attest() {
    let error = AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_memory_ingress(&memory_with(deployment_chain(7)))
    .expect_err("an authority with no chain has nothing to attest about");

    assert_eq!(error.code(), "guardrail-chain-mismatch");
}

/// Replacing the chain after attesting revokes the attestation.
///
/// An attestation is about one particular declared chain; a replacement has
/// not been checked against anything.
#[test]
fn replacing_the_authority_chain_after_attestation_revokes_it() {
    let chain = deployment_chain(7);
    let authority = AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_guardrails(chain.clone())
    .with_memory_ingress(&memory_with(chain))
    .expect("the attestation holds")
    .with_guardrails(deployment_chain(9));

    assert_eq!(
        authority.evaluated_boundaries(),
        AGENT_AUTHORITY_EVALUATED_GUARDRAIL_BOUNDARIES,
        "the replaced chain was never checked against a bundle"
    );
}

/// Two chains built independently from the same declaration attest.
///
/// The check compares declarations, not object identity, so a deployment that
/// legitimately builds its chain twice from one configuration is not punished
/// for it.
#[test]
fn two_independently_built_chains_with_the_same_declaration_attest() {
    AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_guardrails(deployment_chain(7))
    .with_memory_ingress(&memory_with(deployment_chain(7)))
    .expect("two equal declarations are one declared evaluation");
}

// ---------------------------------------------------------------------------
// And the end-to-end consequence, through the real dispatch pipeline.
// ---------------------------------------------------------------------------

/// A deployment that requires a memory-ingress-only stage and forgets to
/// attest fails closed at dispatch rather than losing the protection quietly.
#[tokio::test]
async fn an_unattested_deployment_refuses_dispatch_for_a_memory_ingress_only_mandatory_stage() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let mut envelope = envelope_for_registry(&registry);
    envelope.mandatory_guardrails.insert(stage_id(STAGE));

    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn())
        .with_turn_for(2, proposing_turn());
    let fx = AuthorityFixture::new(
        adapter,
        AgentToolAuthority::new(registry).with_guardrails(deployment_chain(7)),
        None,
    )
    .with_envelope(envelope);
    fx.start().await;
    fx.pump().await;

    assert_eq!(
        fx.terminal_failure_code().await,
        "guardrail-stage-unevaluated",
        "an unattested deployment must fail closed, not run unprotected"
    );
    assert_eq!(fx.adapter.calls(), 0, "the model boundary is guarded too");
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
}

/// The same deployment, attested, dispatches.
///
/// The positive control: without it, the test above would pass for a runtime
/// that refused everything.
#[tokio::test]
async fn the_same_deployment_attested_dispatches() {
    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let mut envelope = envelope_for_registry(&registry);
    envelope.mandatory_guardrails.insert(stage_id(STAGE));

    let chain = deployment_chain(7);
    let authority = AgentToolAuthority::new(registry)
        .with_guardrails(chain.clone())
        .with_memory_ingress(&memory_with(chain))
        .expect("the attestation holds");

    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn())
        .with_turn_for(2, proposing_turn());
    let fx = AuthorityFixture::new(adapter, authority, None).with_envelope(envelope);
    fx.start().await;
    fx.pump().await;

    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "an attested deployment dispatches normally"
    );
}

/// A sanity check on the two constants, so a future edit cannot quietly make
/// the attestation a no-op by widening the unattested set.
#[test]
fn the_unattested_boundary_set_is_strictly_smaller() {
    assert!(
        AGENT_AUTHORITY_EVALUATED_GUARDRAIL_BOUNDARIES.len()
            < AGENT_EVALUATED_GUARDRAIL_BOUNDARIES.len(),
        "if the two sets were equal the attestation would grant nothing"
    );
    for boundary in AGENT_AUTHORITY_EVALUATED_GUARDRAIL_BOUNDARIES {
        assert!(
            AGENT_EVALUATED_GUARDRAIL_BOUNDARIES.contains(&boundary),
            "the authority evaluates {boundary}, which the runtime does not claim to"
        );
    }
    let required = BTreeSet::from([stage_id(STAGE)]);
    deployment_chain(7)
        .validate_covers(&required, &AGENT_AUTHORITY_EVALUATED_GUARDRAIL_BOUNDARIES)
        .expect_err("a memory-ingress-only stage covers nothing an authority evaluates alone");
}

// ---------------------------------------------------------------------------
// The attested object is the object the run assembles through.
// ---------------------------------------------------------------------------

/// A stage that counts the memory-ingress evaluations it performs, so a test
/// can assert the chain *ran* rather than that a constant named it.
struct CountingIngress(Arc<AtomicUsize>);

impl AgentGuardrail for CountingIngress {
    fn evaluate(
        &self,
        context: &AgentGuardrailContext<'_>,
        _content: &serde_json::Value,
    ) -> AgentGuardrailOutcome {
        if context.boundary == AgentGuardrailBoundary::MemoryIngress {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        AgentGuardrailOutcome::Allow
    }
}

/// [`deployment_chain`], with a stage that records that it ran.
fn counting_chain(revision: u64, seen: Arc<AtomicUsize>) -> AgentGuardrailChain {
    AgentGuardrailChain::new(AgentRevisionNumber::new(revision))
        .with_stage(
            AgentGuardrailStage::new(
                stage_id(STAGE),
                AgentRevisionNumber::INITIAL,
                Arc::new(CountingIngress(seen)),
            )
            .at_boundary(AgentGuardrailBoundary::MemoryIngress)
            .mandatory(),
        )
        .expect("the stage registers")
}

fn private_memory(name: &str, text: &str) -> AgentPrivateMemory {
    AgentPrivateMemory::new(
        AgentPrivateMemoryId::new(format!("mem-{name}")).expect("memory id"),
        MemoryOperationId::derive_for_agent(&agent_scope(), format!("create-{name}"))
            .expect("op id"),
        AgentPrivateMemoryKind::Semantic,
        AgentTaskContent::inline(serde_json::json!(text)).expect("content"),
        9_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the memory is bounded")
}

/// A run memory whose retriever deterministically ranks one seeded record, so
/// the ingress chain is handed something to evaluate.
///
/// A scripted retriever rather than the in-memory one: the ranking must not
/// depend on token overlap between the fixture's task input and the seed, and
/// the record still has to survive the authoritative-store resolution, which
/// is what the seeding is for.
fn memory_ranking(
    store: Arc<InMemoryAgentPrivateMemoryStore>,
    chain: AgentGuardrailChain,
    ranked: AgentPrivateMemory,
) -> AgentRunMemory {
    let outcome = MemoryRetrievalOutcome {
        memories: vec![RetrievedPrivateMemory {
            memory: ranked,
            relevance_bps: 9_000,
            embedding: None,
        }],
        index_watermark: None,
    };
    AgentRunMemory::new(
        Arc::new(InMemorySessionMemoryStore::new()),
        Arc::new(InMemoryContextSnapshotStore::new()),
    )
    .with_retrieval(AgentMemoryRetrieval::new(
        Arc::new(ScriptedPrivateMemoryRetriever::new().with_outcome(outcome)),
        store,
        chain,
    ))
}

/// The positive control, wired the way a deployment actually runs: the
/// attested memory is the memory the run assembles through, and the stage the
/// envelope requires is observed running.
///
/// The previous version of this test attested a bundle built for the occasion
/// and then handed the fixture no memory at all. It passed — dispatch
/// succeeded with `MemoryIngress` counted as evaluated — while no ingress
/// stage existed anywhere in the run. That is precisely the fail-open the
/// attestation was added to close, reproduced inside its own positive control.
#[tokio::test]
async fn the_attested_memory_is_the_one_the_run_assembles_through() {
    let seen = Arc::new(AtomicUsize::new(0));
    let chain = counting_chain(7, seen.clone());

    let store = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    let record = private_memory("ours", "the renewal terms");
    store
        .upsert(&agent_scope(), &record, PrivateMemoryExpectation::Absent)
        .await
        .expect("the seed upserts");
    let memory = memory_ranking(store, chain.clone(), record);

    let registry = tool_registry_for_spec(TOOL, &AgentEffectSpec::non_idempotent());
    let mut envelope = envelope_for_registry(&registry);
    envelope.mandatory_guardrails.insert(stage_id(STAGE));

    let authority = AgentToolAuthority::new(registry)
        .with_guardrails(chain)
        .with_memory_ingress(&memory)
        .expect("the attestation holds");
    assert!(
        authority.attests(&memory),
        "the attestation must hold for the memory it was shown"
    );

    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn())
        .with_turn_for(2, proposing_turn());
    let fx = AuthorityFixture::new(adapter, authority, None)
        .with_envelope(envelope)
        .with_memory(memory);
    fx.start().await;
    fx.pump().await;

    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "an attested deployment dispatches normally"
    );
    assert!(
        seen.load(Ordering::SeqCst) >= 1,
        "dispatch was admitted on a memory-ingress stage that never ran"
    );
}

/// A memory carrying no retrieval bundle cannot be attested.
///
/// This is the shape the old positive control silently had: nothing evaluates
/// the boundary, so there is no chain to be the same as.
#[test]
fn a_memory_with_no_retrieval_bundle_cannot_be_attested() {
    let error = AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_guardrails(deployment_chain(7))
    .with_memory_ingress(&memory_without_retrieval())
    .expect_err("a memory with no bundle evaluates the boundary nowhere");

    assert_eq!(error.code(), "guardrail-chain-mismatch");
}

/// Two absences are not an agreement.
///
/// An authority with no chain and a memory with no bundle both declare
/// nothing, so a bare declaration comparison finds them equal — and would
/// attest that a boundary nobody evaluates is evaluated. The emptiest possible
/// deployment is exactly the one that must not pass.
#[test]
fn an_authority_with_no_chain_cannot_attest_a_memory_with_no_bundle() {
    let error = AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_memory_ingress(&memory_without_retrieval())
    .expect_err("neither side evaluates the boundary, so neither can vouch for it");

    assert_eq!(error.code(), "guardrail-chain-mismatch");
}

/// The attestation can be re-checked against a memory assembled elsewhere.
///
/// The check runs once, at wiring time. A deployment that builds its run
/// memory in a second place — a reload, a refactor, a per-shard construction —
/// has an assertion available rather than an assumption.
#[test]
fn attests_re_checks_a_separately_assembled_memory() {
    let chain = deployment_chain(7);
    let authority = AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_guardrails(chain.clone())
    .with_memory_ingress(&memory_with(chain))
    .expect("the attestation holds");

    assert!(
        authority.attests(&memory_with(deployment_chain(7))),
        "an equal declaration is the same declared evaluation, however it was built"
    );
    assert!(
        !authority.attests(&memory_with(AgentGuardrailChain::new(
            AgentRevisionNumber::new(7)
        ))),
        "an empty chain at the same revision runs no stage"
    );
    assert!(
        !authority.attests(&memory_without_retrieval()),
        "a memory with no bundle evaluates the boundary nowhere"
    );

    let unattested = AgentToolAuthority::new(tool_registry_for_spec(
        TOOL,
        &AgentEffectSpec::non_idempotent(),
    ))
    .with_guardrails(deployment_chain(7));
    assert!(
        !unattested.attests(&memory_with(deployment_chain(7))),
        "an authority that never attested vouches for nothing, matching chain or not"
    );
}
