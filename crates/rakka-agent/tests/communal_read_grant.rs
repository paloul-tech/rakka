//! What a run may read communally: its effective read grant.
//!
//! Issue #69, [specification 7.3, 8.5 and 13.4](../../../docs/plans/rakka-agent/spec.md).
//! The grant is read from what the dispatcher reads — the agent's durable
//! definition, and the run's setup when it has one — and is then narrowed by
//! the run's own delegation envelope, with the rule a communal append already
//! follows. Nothing here is taken from model output.

use std::collections::BTreeSet;
use std::sync::Arc;

use rakka_agent::testkit::ScriptedDispatcher;
use rakka_agent::{
    load_agent_entity_state, AgentAuthorityEnvelope, AgentCapabilityId, AgentCommunalReadGrant,
    AgentCommunalReadGrantSource, AgentDefinition, AgentDefinitionId, AgentEntityCommand,
    AgentEntityCommunalReadGrants, AgentEntityStore, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentRunDelegationEnvelope, AgentRunScope, AgentRunSetupResolver,
    AgentSchemaPolicy, AgentSetupRevision, KnowledgeSpaceId,
};
use rakka_agent_workflow::AgentTimestampMillis;

mod common;

use common::*;

const NOW: AgentTimestampMillis = AgentTimestampMillis::new(50);

fn spaces(ids: &[&str]) -> BTreeSet<KnowledgeSpaceId> {
    ids.iter()
        .map(|id| KnowledgeSpaceId::new(*id).expect("the space id is valid"))
        .collect()
}

fn capabilities(ids: &[&str]) -> BTreeSet<AgentCapabilityId> {
    ids.iter()
        .map(|id| AgentCapabilityId::new(*id).expect("the capability id is valid"))
        .collect()
}

/// An envelope granting the given spaces and read capabilities beside what
/// the fixture agent needs.
fn envelope(space_ids: &[&str], capability_ids: &[&str]) -> AgentAuthorityEnvelope {
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope.task_definitions.insert(task_definition_id());
    envelope.knowledge_spaces = spaces(space_ids);
    envelope.read_capabilities = capabilities(capability_ids);
    envelope
}

fn fixture() -> Fixture {
    Fixture::new(ScriptedDispatcher::new())
}

fn grants(fx: &Fixture) -> AgentEntityCommunalReadGrants<AgentStore> {
    AgentEntityCommunalReadGrants::new(Arc::new(fx.agents.clone()), AgentSchemaPolicy::default())
}

/// A resolver that answers one setup for the fixture run and none for any
/// other.
struct FixtureRunSetup(AgentSetupRevision);

impl AgentRunSetupResolver for FixtureRunSetup {
    fn setup_for(&self, scope: &AgentRunScope) -> Option<AgentSetupRevision> {
        (scope == &run_scope()).then(|| self.0.clone())
    }
}

/// A setup over the agent's current definition, narrowed to `narrowed`.
async fn setup_over_current_definition(
    fx: &Fixture,
    narrowed: AgentAuthorityEnvelope,
) -> AgentSetupRevision {
    let state = load_agent_entity_state(&fx.agents, &agent_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the agent state loads")
        .expect("the agent exists");
    AgentSetupRevision::new(
        AgentRevisionNumber::new(7),
        state.definition(),
        narrowed,
        provenance(2),
    )
    .expect("the setup narrows the definition")
}

async fn lifecycle(fx: &Fixture, discriminator: &str, command: &str) {
    let mut agent = AgentEntityStore::new(agent_scope(), fx.agents.clone());
    agent.recover().await.expect("the agent recovers");
    let expected_lifecycle_revision = agent
        .state()
        .expect("the state reads")
        .expect("the agent exists")
        .lifecycle_revision();
    let operation_id = AgentOperationId::for_agent(
        AgentOperationKind::LifecycleCommand,
        &agent_scope(),
        discriminator,
    )
    .expect("the operation id derives");
    let provenance = Box::new(provenance(90));
    let command = match command {
        "suspend" => AgentEntityCommand::Suspend {
            operation_id,
            expected_lifecycle_revision,
            provenance,
        },
        "resume" => AgentEntityCommand::Resume {
            operation_id,
            expected_lifecycle_revision,
            provenance,
        },
        _ => AgentEntityCommand::Terminate {
            operation_id,
            expected_lifecycle_revision,
            provenance,
        },
    };
    agent
        .apply(command)
        .await
        .expect("the lifecycle command applies");
}

#[tokio::test]
async fn a_definition_alone_grants_its_spaces_and_read_capabilities() {
    let fx = fixture();
    fx.instantiate_agent_with_envelope(envelope(&["s1", "s2"], &["cap-a", "cap-b"]))
        .await;
    let source = grants(&fx);
    assert_eq!(source.backend_name(), "agent-entity");

    assert_eq!(
        source
            .read_grant(&run_scope(), NOW)
            .await
            .expect("the grant reads"),
        Some(AgentCommunalReadGrant {
            spaces: spaces(&["s1", "s2"]),
            read_capabilities: capabilities(&["cap-a", "cap-b"]),
            definition_revision: AgentRevisionNumber::INITIAL,
            setup_revision: None,
        })
    );
}

#[tokio::test]
async fn a_setup_narrows_the_definition() {
    let fx = fixture();
    fx.instantiate_agent_with_envelope(envelope(&["s1", "s2"], &["cap-a", "cap-b"]))
        .await;
    let setup = setup_over_current_definition(&fx, envelope(&["s1"], &["cap-a"])).await;
    let source = grants(&fx).with_setups(Arc::new(FixtureRunSetup(setup)));

    assert_eq!(
        source
            .read_grant(&run_scope(), NOW)
            .await
            .expect("the grant reads"),
        Some(AgentCommunalReadGrant {
            spaces: spaces(&["s1"]),
            read_capabilities: capabilities(&["cap-a"]),
            definition_revision: AgentRevisionNumber::INITIAL,
            setup_revision: Some(AgentRevisionNumber::new(7)),
        })
    );

    // The setup is the fixture run's: another run of the same agent, created
    // under no setup, reads under the definition alone.
    let other = AgentRunScope::new(
        tenant(),
        agent_id(),
        rakka_agent::AgentRunId::new("another-run").expect("the run id is valid"),
    )
    .expect("the scope is valid");
    let grant = source
        .read_grant(&other, NOW)
        .await
        .expect("the grant reads")
        .expect("the agent exists");
    assert_eq!(grant.spaces, spaces(&["s1", "s2"]));
    assert_eq!(grant.setup_revision, None);
}

/// A setup is admitted against one definition revision. When the definition
/// is narrowed afterwards, the narrower definition still binds: the grant is
/// what both allow.
#[tokio::test]
async fn a_definition_narrowed_below_its_setup_still_binds() {
    let fx = fixture();
    fx.instantiate_agent_with_envelope(envelope(&["s1", "s2"], &["cap-a", "cap-b"]))
        .await;
    let setup =
        setup_over_current_definition(&fx, envelope(&["s1", "s2"], &["cap-a", "cap-b"])).await;

    let mut agent = AgentEntityStore::new(agent_scope(), fx.agents.clone());
    agent.recover().await.expect("the agent recovers");
    let narrowed = AgentDefinition::new(
        AgentDefinitionId::new("narrowed-v2").expect("the definition id is valid"),
        "The same agent, granted less.",
        envelope(&["s2"], &["cap-b"]),
    )
    .expect("the definition is valid");
    agent
        .apply(AgentEntityCommand::PublishDefinition {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::DefinitionUpdate,
                &agent_scope(),
                "2",
            )
            .expect("the operation id derives"),
            definition: Box::new(narrowed),
            provenance: Box::new(provenance(3)),
        })
        .await
        .expect("the narrower definition publishes");

    let grant = grants(&fx)
        .with_setups(Arc::new(FixtureRunSetup(setup)))
        .read_grant(&run_scope(), NOW)
        .await
        .expect("the grant reads")
        .expect("the agent exists");
    assert_eq!(grant.spaces, spaces(&["s2"]));
    assert_eq!(grant.read_capabilities, capabilities(&["cap-b"]));
    assert_eq!(grant.definition_revision, AgentRevisionNumber::new(2));
    assert_eq!(grant.setup_revision, Some(AgentRevisionNumber::new(7)));
}

#[tokio::test]
async fn an_agent_that_does_not_exist_has_no_grant() {
    let fx = fixture();
    assert_eq!(
        grants(&fx)
            .read_grant(&run_scope(), NOW)
            .await
            .expect("the grant reads"),
        None
    );
}

/// An agent the dispatcher would refuse to dispatch for reads nothing: a
/// suspended agent until it resumes, a terminated agent ever.
#[tokio::test]
async fn an_agent_the_dispatcher_would_refuse_has_no_grant() {
    let fx = fixture();
    fx.instantiate_agent_with_envelope(envelope(&["s1"], &["cap-a"]))
        .await;
    let source = grants(&fx);
    assert!(source
        .read_grant(&run_scope(), NOW)
        .await
        .expect("the grant reads")
        .is_some());

    lifecycle(&fx, "suspend-1", "suspend").await;
    assert_eq!(
        source
            .read_grant(&run_scope(), NOW)
            .await
            .expect("the grant reads"),
        None,
        "a suspended agent has no grant"
    );

    lifecycle(&fx, "resume-1", "resume").await;
    assert!(source
        .read_grant(&run_scope(), NOW)
        .await
        .expect("the grant reads")
        .is_some());

    lifecycle(&fx, "terminate-1", "terminate").await;
    assert_eq!(
        source
            .read_grant(&run_scope(), NOW)
            .await
            .expect("the grant reads"),
        None,
        "a terminated agent has no grant"
    );
}

// ---------------------------------------------------------------------------
// The run's delegation envelope narrows the answer.
// ---------------------------------------------------------------------------

fn answered() -> AgentCommunalReadGrant {
    AgentCommunalReadGrant {
        spaces: spaces(&["s1", "s2"]),
        read_capabilities: capabilities(&["cap-a", "cap-b"]),
        definition_revision: AgentRevisionNumber::new(3),
        setup_revision: Some(AgentRevisionNumber::new(7)),
    }
}

fn lineage() -> Vec<rakka_agent::AgentDelegationId> {
    vec![rakka_agent::delegation_id_for(&run_scope(), 1, 0).expect("the delegation id derives")]
}

#[test]
fn a_delegation_statement_intersects_the_grant() {
    let envelope = AgentRunDelegationEnvelope {
        knowledge_spaces: Some(spaces(&["s2", "s3"])),
        read_capabilities: Some(capabilities(&["cap-b", "cap-c"])),
        lineage: lineage(),
        ..Default::default()
    };
    let effective = answered().narrowed_by(Some(&envelope));
    assert_eq!(effective.spaces, spaces(&["s2"]));
    assert_eq!(effective.read_capabilities, capabilities(&["cap-b"]));
    // What the grant was read under is carried through unchanged.
    assert_eq!(effective.definition_revision, AgentRevisionNumber::new(3));
    assert_eq!(effective.setup_revision, Some(AgentRevisionNumber::new(7)));

    // A statement binds at the root too, and an explicit empty statement is
    // explicitly nothing.
    let root = AgentRunDelegationEnvelope {
        knowledge_spaces: Some(spaces(&["s1"])),
        read_capabilities: Some(BTreeSet::new()),
        ..Default::default()
    };
    let effective = answered().narrowed_by(Some(&root));
    assert_eq!(effective.spaces, spaces(&["s1"]));
    assert!(effective.read_capabilities.is_empty());
}

#[test]
fn lineage_without_a_statement_grants_nothing() {
    let envelope = AgentRunDelegationEnvelope {
        lineage: lineage(),
        ..Default::default()
    };
    let effective = answered().narrowed_by(Some(&envelope));
    assert!(effective.spaces.is_empty());
    assert!(effective.read_capabilities.is_empty());

    // Each set follows the rule on its own: a chain that states its spaces
    // and not its read capabilities reads those spaces with no capability.
    let half = AgentRunDelegationEnvelope {
        knowledge_spaces: Some(spaces(&["s1"])),
        lineage: lineage(),
        ..Default::default()
    };
    let effective = answered().narrowed_by(Some(&half));
    assert_eq!(effective.spaces, spaces(&["s1"]));
    assert!(effective.read_capabilities.is_empty());
}

#[test]
fn a_root_without_a_statement_keeps_the_grant() {
    let effective = answered().narrowed_by(Some(&AgentRunDelegationEnvelope::default()));
    assert_eq!(effective, answered());
}

#[test]
fn no_envelope_keeps_the_grant() {
    assert_eq!(answered().narrowed_by(None), answered());
}
