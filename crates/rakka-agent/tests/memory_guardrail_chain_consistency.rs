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
use std::sync::Arc;

use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    AgentEffectSpec, AgentGuardrail, AgentGuardrailBoundary, AgentGuardrailChain,
    AgentGuardrailContext, AgentGuardrailOutcome, AgentGuardrailStage, AgentGuardrailStageId,
    AgentMemoryRetrieval, AgentModelTurn, AgentRevisionNumber, AgentTaskContent,
    AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentPrivateMemoryStore, InMemoryPrivateMemoryRetriever,
    AGENT_AUTHORITY_EVALUATED_GUARDRAIL_BOUNDARIES, AGENT_EVALUATED_GUARDRAIL_BOUNDARIES,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

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

fn bundle(chain: AgentGuardrailChain) -> AgentMemoryRetrieval {
    let store = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    AgentMemoryRetrieval::new(
        Arc::new(InMemoryPrivateMemoryRetriever::new(store.clone())),
        store,
        chain,
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
    .with_memory_ingress(&bundle(chain))
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
    .with_memory_ingress(&bundle(deployment_chain(8)))
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
    .with_memory_ingress(&bundle(empty))
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
    .with_memory_ingress(&bundle(deployment_chain(7)))
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
    .with_memory_ingress(&bundle(chain))
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
    .with_memory_ingress(&bundle(deployment_chain(7)))
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
        .with_memory_ingress(&bundle(chain))
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
