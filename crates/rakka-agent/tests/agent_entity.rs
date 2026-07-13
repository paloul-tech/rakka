//! Sharded agent entity contracts.
//!
//! Specification: sections 6.2, 6.10, 6.11, 7.2, and 15. The entity is addressed
//! by `(TenantId, AgentId)`, owns the durable definition, lifecycle status, and
//! settings revision, and must survive passivation and re-materialization on
//! another shard owner with nothing but its durable state.

use std::time::Duration;

use rakka_agent::{
    agent_entity_ref, init_agent_entity_sharding, load_agent_entity_state, passivate_agent_entity,
    registered_agent_entity_ref, AgentAuthorityEnvelope, AgentDefinition, AgentDefinitionId,
    AgentEntityCommand, AgentEntityRef, AgentEntityRegistration, AgentEntityReply,
    AgentEntityShardingSettings, AgentEntitySnapshot, AgentEntityState, AgentEntityStore, AgentId,
    AgentLifecycleStatus, AgentModelProfileId, AgentOperationId, AgentOperationKind,
    AgentRevisionNumber, AgentRevisionProvenance, AgentSchemaPolicy, AgentScope, AgentSettings,
    AgentSettingsChange, AgentToolDeclaration, AgentToolId, TenantId,
    AGENT_ENTITY_OPERATION_LOG_CAPACITY,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};
use rakka_core::ActorSystem;
use rakka_persistence::InMemoryDurableStateStore;
use rakka_sharding::{ClusterSharding, EntityTypeKey, ShardBufferConfig};

type Store = InMemoryDurableStateStore<AgentEntityState>;

const ASK_TIMEOUT: Duration = Duration::from_secs(1);

fn scope() -> AgentScope {
    AgentScope::new(
        TenantId::new("acme"),
        AgentId::new("support-agent").expect("agent id should be valid"),
    )
    .expect("agent scope should be valid")
}

fn provenance(at: u64) -> AgentRevisionProvenance {
    AgentRevisionProvenance {
        principal: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "operator-1".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

fn model(id: &str) -> AgentModelProfileId {
    AgentModelProfileId::new(id).expect("model profile id should be valid")
}

fn definition() -> AgentDefinition {
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope.model_profiles.insert(model("frontier"));
    envelope.model_profiles.insert(model("mini"));
    envelope.tools.insert(
        AgentToolId::new("search").expect("tool id should be valid"),
        AgentToolDeclaration::new(rakka_agent::AgentEffectSafetyClass::ReadOnly),
    );

    AgentDefinition::new(
        AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
        "Resolves customer support tickets end to end.",
        envelope,
    )
    .expect("definition should be valid")
}

fn operation(kind: AgentOperationKind, discriminator: &str) -> AgentOperationId {
    AgentOperationId::for_agent(kind, &scope(), discriminator)
        .expect("operation id should be derivable")
}

fn instantiate_command() -> AgentEntityCommand {
    AgentEntityCommand::Instantiate {
        operation_id: operation(AgentOperationKind::DefinitionUpdate, "1"),
        definition: Box::new(definition()),
        settings: Box::new(AgentSettings {
            model_profile: Some(model("frontier")),
            ..AgentSettings::default()
        }),
        provenance: Box::new(provenance(1)),
    }
}

async fn ask(entity: &AgentEntityRef, command: AgentEntityCommand) -> AgentEntityReply {
    entity
        .ask(
            |reply_to| rakka_agent::AgentEntityMessage { command, reply_to },
            ASK_TIMEOUT,
        )
        .await
        .expect("the agent entity should reply")
}

fn applied(reply: AgentEntityReply) -> rakka_agent::AgentEntityOutcome {
    match reply {
        AgentEntityReply::Applied { outcome } => outcome,
        other => panic!("expected an applied transition, got {other:?}"),
    }
}

fn snapshot(reply: AgentEntityReply) -> AgentEntitySnapshot {
    match reply {
        AgentEntityReply::Snapshot(Some(snapshot)) => *snapshot,
        other => panic!("expected a snapshot, got {other:?}"),
    }
}

fn rejection_code(reply: AgentEntityReply) -> String {
    match reply {
        AgentEntityReply::Rejected { code, .. } => code,
        other => panic!("expected a rejection, got {other:?}"),
    }
}

/// One booted, node-local sharded agent entity type over an in-memory store.
struct Fixture {
    system: ActorSystem,
    sharding: ClusterSharding,
    store: Store,
    registration: AgentEntityRegistration,
    entity: AgentEntityRef,
}

fn sharded_agent(name: &str) -> Fixture {
    let system = ActorSystem::new(name);
    let sharding = ClusterSharding::get(&system);
    let store = Store::new();
    let key = EntityTypeKey::new(name)
        .with_number_of_shards(4)
        .expect("entity type key should be valid");
    // The passivation buffer is left at its default: a command that arrives while
    // the entity is stopping is held and delivered to the re-materialized entity,
    // which is precisely the behavior the recovery test exercises.
    let settings = AgentEntityShardingSettings::new(key)
        .with_idle_passivation(Duration::from_secs(60))
        .with_buffering(ShardBufferConfig::new(8, Duration::from_millis(250)));

    let registration = init_agent_entity_sharding(&sharding, store.clone(), settings)
        .expect("agent entity sharding should initialize");
    let entity = registered_agent_entity_ref(&registration, &scope());

    // The entity routes by its scope key, so any holder of the scope reaches the
    // same entity without knowing where it lives.
    let routed = agent_entity_ref(&sharding, registration.key(), &scope())
        .expect("the scope should route to an entity");
    assert_eq!(routed.entity_id(), entity.entity_id());
    assert_eq!(entity.entity_id().as_str(), "acme/support-agent");

    Fixture {
        system,
        sharding,
        store,
        registration,
        entity,
    }
}

#[tokio::test]
async fn an_agent_persists_passivates_and_recovers_its_definition_and_settings() {
    let Fixture {
        system,
        sharding,
        store,
        registration,
        entity,
    } = sharded_agent("AgentEntityRecovery");

    let outcome = applied(ask(&entity, instantiate_command()).await);
    assert_eq!(outcome.status, AgentLifecycleStatus::Active);
    assert_eq!(outcome.definition_revision, AgentRevisionNumber::INITIAL);
    assert_eq!(outcome.settings_revision, AgentRevisionNumber::INITIAL);

    let outcome = applied(
        ask(
            &entity,
            AgentEntityCommand::UpdateSettings {
                operation_id: operation(AgentOperationKind::SettingsUpdate, "2"),
                expected_revision: AgentRevisionNumber::INITIAL,
                changes: vec![AgentSettingsChange::ModelProfile(model("mini"))],
                provenance: Box::new(provenance(2)),
            },
        )
        .await,
    );
    assert_eq!(outcome.settings_revision, AgentRevisionNumber::new(2));

    // Passivate: the actor and every byte of its in-memory state go away. An
    // `Active` agent with no actor instance on any pod is the normal resting
    // state, not a degraded one (specification 6.11).
    assert!(
        passivate_agent_entity(&sharding, registration.key(), &scope())
            .expect("passivation should be routable"),
        "the entity should have been resident"
    );
    assert_eq!(
        sharding
            .registration_state(registration.key())
            .expect("registration state should exist")
            .local_entity_count(),
        0,
        "a passivated agent holds no per-agent runtime resources"
    );

    // The next command re-materializes the entity, which must recover its
    // definition and settings from durable state alone.
    let recovered = snapshot(ask(&entity, AgentEntityCommand::Describe).await);
    assert_eq!(recovered.status, AgentLifecycleStatus::Active);
    assert_eq!(recovered.lifecycle_revision, AgentRevisionNumber::INITIAL);
    assert_eq!(recovered.definition_revision, AgentRevisionNumber::INITIAL);
    assert_eq!(recovered.settings_revision, AgentRevisionNumber::new(2));
    assert_eq!(recovered.scope, scope());
    assert_eq!(
        recovered.memory_namespace.as_str(),
        "agent-memory/acme/support-agent"
    );
    assert_eq!(
        recovered.description,
        "Resolves customer support tickets end to end."
    );

    // The durable record is the source of truth, and the assignment read path
    // sees the same state without waking the entity at all.
    let durable = load_agent_entity_state(&store, &scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the durable state should load")
        .expect("the agent was instantiated");
    assert_eq!(durable.settings().revision(), AgentRevisionNumber::new(2));
    assert_eq!(
        durable.settings().settings().model_profile,
        Some(model("mini"))
    );
    assert!(durable.is_dispatch_permitted());

    system.shutdown();
}

#[tokio::test]
async fn a_replayed_command_returns_its_original_outcome_without_transitioning_twice() {
    let Fixture {
        system,
        store,
        entity,
        ..
    } = sharded_agent("AgentEntityReplay");

    applied(ask(&entity, instantiate_command()).await);

    let update = AgentEntityCommand::UpdateSettings {
        operation_id: operation(AgentOperationKind::SettingsUpdate, "2"),
        expected_revision: AgentRevisionNumber::INITIAL,
        changes: vec![AgentSettingsChange::ModelProfile(model("mini"))],
        provenance: Box::new(provenance(2)),
    };

    let first = applied(ask(&entity, update.clone()).await);
    assert_eq!(first.settings_revision, AgentRevisionNumber::new(2));

    // An initiator that crashed before recording the reply re-drives the same
    // operation id. The entity must return the original result, not apply the
    // change a second time.
    let replay = ask(&entity, update).await;
    match replay {
        AgentEntityReply::Duplicate { outcome } => assert_eq!(outcome, first),
        other => panic!("a replayed operation id must be deduplicated, got {other:?}"),
    }

    let durable = load_agent_entity_state(&store, &scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the durable state should load")
        .expect("the agent was instantiated");
    assert_eq!(
        durable.settings().revision(),
        AgentRevisionNumber::new(2),
        "the replay must not have produced a second revision"
    );

    system.shutdown();
}

#[tokio::test]
async fn a_settings_update_against_a_stale_revision_is_rejected() {
    let Fixture { system, entity, .. } = sharded_agent("AgentEntityStaleSettings");

    applied(ask(&entity, instantiate_command()).await);
    applied(
        ask(
            &entity,
            AgentEntityCommand::UpdateSettings {
                operation_id: operation(AgentOperationKind::SettingsUpdate, "2"),
                expected_revision: AgentRevisionNumber::INITIAL,
                changes: vec![AgentSettingsChange::ModelProfile(model("mini"))],
                provenance: Box::new(provenance(2)),
            },
        )
        .await,
    );

    // A second writer that read revision 1 must not silently overwrite the
    // revision-2 decision it never saw.
    let code = rejection_code(
        ask(
            &entity,
            AgentEntityCommand::UpdateSettings {
                operation_id: operation(AgentOperationKind::SettingsUpdate, "3"),
                expected_revision: AgentRevisionNumber::INITIAL,
                changes: vec![AgentSettingsChange::RetrievalLimit(4)],
                provenance: Box::new(provenance(3)),
            },
        )
        .await,
    );
    assert_eq!(code, "stale-settings-revision");

    system.shutdown();
}

#[tokio::test]
async fn a_write_that_lands_behind_the_entity_is_rejected_and_then_reconciled() {
    let Fixture {
        system,
        store,
        entity,
        ..
    } = sharded_agent("AgentEntityRevisionConflict");

    applied(ask(&entity, instantiate_command()).await);

    // Something else — a migration job, a stale shard owner — writes this agent's
    // durable state behind the live entity's back.
    let mut out_of_band = AgentEntityStore::new(scope(), store.clone());
    out_of_band
        .recover()
        .await
        .expect("the out-of-band writer should recover the state");
    out_of_band
        .apply(AgentEntityCommand::UpdateSettings {
            operation_id: operation(AgentOperationKind::SettingsUpdate, "out-of-band"),
            expected_revision: AgentRevisionNumber::INITIAL,
            changes: vec![AgentSettingsChange::RetrievalLimit(9)],
            provenance: Box::new(provenance(2)),
        })
        .await
        .expect("the out-of-band write should apply");

    // The entity's cached record is now stale. Its next write must be rejected
    // rather than clobbering the revision it never saw.
    let code = rejection_code(
        ask(
            &entity,
            AgentEntityCommand::UpdateSettings {
                operation_id: operation(AgentOperationKind::SettingsUpdate, "3"),
                expected_revision: AgentRevisionNumber::INITIAL,
                changes: vec![AgentSettingsChange::ModelProfile(model("mini"))],
                provenance: Box::new(provenance(3)),
            },
        )
        .await,
    );
    assert_eq!(code, "revision-conflict");

    // And the entity must then converge: the rejection dropped its stale cache, so
    // the next command reloads the authoritative record and applies against it.
    let outcome = applied(
        ask(
            &entity,
            AgentEntityCommand::UpdateSettings {
                operation_id: operation(AgentOperationKind::SettingsUpdate, "4"),
                expected_revision: AgentRevisionNumber::new(2),
                changes: vec![AgentSettingsChange::ModelProfile(model("mini"))],
                provenance: Box::new(provenance(4)),
            },
        )
        .await,
    );
    assert_eq!(outcome.settings_revision, AgentRevisionNumber::new(3));

    let durable = load_agent_entity_state(&store, &scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the durable state should load")
        .expect("the agent was instantiated");
    assert_eq!(
        durable.settings().settings().retrieval_limit,
        Some(9),
        "the out-of-band decision must survive: the entity reconciled with it, not over it"
    );
    assert_eq!(
        durable.settings().settings().model_profile,
        Some(model("mini"))
    );

    system.shutdown();
}

#[tokio::test]
async fn suspension_withdraws_dispatch_permission_and_termination_is_terminal() {
    let Fixture {
        system,
        store,
        entity,
        ..
    } = sharded_agent("AgentEntityLifecycle");

    applied(ask(&entity, instantiate_command()).await);

    let outcome = applied(
        ask(
            &entity,
            AgentEntityCommand::Suspend {
                operation_id: operation(AgentOperationKind::LifecycleCommand, "suspend-1"),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
                provenance: Box::new(provenance(2)),
            },
        )
        .await,
    );
    assert_eq!(outcome.status, AgentLifecycleStatus::Suspended);
    assert_eq!(outcome.lifecycle_revision, AgentRevisionNumber::new(2));

    // Suspension is an immediate-safety control: a dispatcher reading durable
    // state, without waking the entity, must see that dispatch is withdrawn.
    let durable = load_agent_entity_state(&store, &scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the durable state should load")
        .expect("the agent was instantiated");
    assert!(!durable.is_dispatch_permitted());

    // A lifecycle command carries the lifecycle revision it expects to advance.
    // A caller that read revision 1 never saw the suspension, so its resume must
    // not reorder over it.
    let code = rejection_code(
        ask(
            &entity,
            AgentEntityCommand::Resume {
                operation_id: operation(AgentOperationKind::LifecycleCommand, "resume-stale"),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
                provenance: Box::new(provenance(3)),
            },
        )
        .await,
    );
    assert_eq!(code, "stale-lifecycle-revision");

    let outcome = applied(
        ask(
            &entity,
            AgentEntityCommand::Resume {
                operation_id: operation(AgentOperationKind::LifecycleCommand, "resume-1"),
                expected_lifecycle_revision: AgentRevisionNumber::new(2),
                provenance: Box::new(provenance(3)),
            },
        )
        .await,
    );
    assert_eq!(outcome.status, AgentLifecycleStatus::Active);

    applied(
        ask(
            &entity,
            AgentEntityCommand::Terminate {
                operation_id: operation(AgentOperationKind::LifecycleCommand, "terminate-1"),
                expected_lifecycle_revision: AgentRevisionNumber::new(3),
                provenance: Box::new(provenance(4)),
            },
        )
        .await,
    );

    let code = rejection_code(
        ask(
            &entity,
            AgentEntityCommand::Resume {
                operation_id: operation(AgentOperationKind::LifecycleCommand, "resume-2"),
                expected_lifecycle_revision: AgentRevisionNumber::new(4),
                provenance: Box::new(provenance(5)),
            },
        )
        .await,
    );
    assert_eq!(code, "agent-terminated");

    system.shutdown();
}

#[tokio::test]
async fn a_published_definition_may_not_strand_the_settings_already_in_force() {
    let Fixture { system, entity, .. } = sharded_agent("AgentEntityDefinitionNarrowing");

    applied(ask(&entity, instantiate_command()).await);

    // The agent currently runs on the "frontier" profile. A definition that no
    // longer approves it would leave the agent dispatching against a model its
    // own definition rejects, so the publication fails closed.
    let mut narrowed = AgentAuthorityEnvelope::empty();
    narrowed.model_profiles.insert(model("mini"));
    let definition = AgentDefinition::new(
        AgentDefinitionId::new("support-v1").expect("definition id should be valid"),
        "Resolves customer support tickets end to end.",
        narrowed,
    )
    .expect("definition should be valid");

    let code = rejection_code(
        ask(
            &entity,
            AgentEntityCommand::PublishDefinition {
                operation_id: operation(AgentOperationKind::DefinitionUpdate, "2"),
                definition: Box::new(definition),
                provenance: Box::new(provenance(2)),
            },
        )
        .await,
    );
    assert_eq!(code, "envelope-widened");

    // The rejected publication left no trace, so the corrected retry — same
    // operation id, wider envelope — is still accepted.
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope.model_profiles.insert(model("frontier"));
    envelope.model_profiles.insert(model("mini"));
    let definition = AgentDefinition::new(
        AgentDefinitionId::new("support-v2").expect("definition id should be valid"),
        "Resolves customer support tickets, and escalates billing disputes.",
        envelope,
    )
    .expect("definition should be valid");

    let outcome = applied(
        ask(
            &entity,
            AgentEntityCommand::PublishDefinition {
                operation_id: operation(AgentOperationKind::DefinitionUpdate, "2"),
                definition: Box::new(definition),
                provenance: Box::new(provenance(3)),
            },
        )
        .await,
    );
    assert_eq!(outcome.definition_revision, AgentRevisionNumber::new(2));
    assert_eq!(
        outcome.settings_revision,
        AgentRevisionNumber::INITIAL,
        "publishing a definition does not disturb the settings revision"
    );

    system.shutdown();
}

#[tokio::test]
async fn a_definition_that_bypasses_construction_is_rejected_at_the_entity() {
    let Fixture { system, entity, .. } = sharded_agent("AgentEntityDefinitionBounds");

    applied(ask(&entity, instantiate_command()).await);

    // The definition's fields are public, so a caller can assemble one without
    // `AgentDefinition::new` — and a node-local command never crosses the
    // deserialization boundary that would otherwise catch it. The entity
    // re-validates at its accept path, so the bounded-description invariant
    // holds no matter how the value was built.
    let mut bypassed = definition();
    bypassed.description = "x".repeat(rakka_agent::AGENT_DESCRIPTION_MAX_LENGTH + 1);

    let code = rejection_code(
        ask(
            &entity,
            AgentEntityCommand::PublishDefinition {
                operation_id: operation(AgentOperationKind::DefinitionUpdate, "bypass"),
                definition: Box::new(bypassed),
                provenance: Box::new(provenance(2)),
            },
        )
        .await,
    );
    assert_eq!(code, "agent-description-too-long");

    // The rejection left no trace: the definition on record is untouched.
    let recovered = snapshot(ask(&entity, AgentEntityCommand::Describe).await);
    assert_eq!(recovered.definition_revision, AgentRevisionNumber::INITIAL);
    assert_eq!(
        recovered.description,
        "Resolves customer support tickets end to end."
    );

    system.shutdown();
}

#[tokio::test]
async fn an_entity_id_that_is_not_a_scope_key_rejects_every_command() {
    let system = ActorSystem::new("AgentEntityMalformedId");
    let sharding = ClusterSharding::get(&system);
    let key = EntityTypeKey::new("AgentEntityMalformedId")
        .with_number_of_shards(4)
        .expect("entity type key should be valid");
    let registration = init_agent_entity_sharding(
        &sharding,
        Store::new(),
        AgentEntityShardingSettings::new(key),
    )
    .expect("agent entity sharding should initialize");

    // An entity id that carries no tenant cannot address a durable record. The
    // entity fails closed rather than guessing a scope.
    let entity = registration.entity_ref_for("support-agent");
    let code = rejection_code(ask(&entity, AgentEntityCommand::Describe).await);
    assert_eq!(code, "malformed-scope-key");

    system.shutdown();
}

#[tokio::test]
async fn commanding_an_agent_that_was_never_instantiated_fails_closed() {
    let Fixture { system, entity, .. } = sharded_agent("AgentEntityNotInstantiated");

    match ask(&entity, AgentEntityCommand::Describe).await {
        AgentEntityReply::Snapshot(None) => {}
        other => panic!("an uninstantiated agent has no snapshot, got {other:?}"),
    }

    let code = rejection_code(
        ask(
            &entity,
            AgentEntityCommand::Suspend {
                operation_id: operation(AgentOperationKind::LifecycleCommand, "suspend-1"),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
                provenance: Box::new(provenance(2)),
            },
        )
        .await,
    );
    assert_eq!(code, "agent-not-instantiated");

    applied(ask(&entity, instantiate_command()).await);

    // Replaying the *same* instantiation is deduplicated and returns the original
    // outcome.
    match ask(&entity, instantiate_command()).await {
        AgentEntityReply::Duplicate { .. } => {}
        other => panic!("a replayed instantiation must be deduplicated, got {other:?}"),
    }

    // A *different* instantiation, however, is a genuine second attempt to create
    // an agent that already exists, and it must not overwrite the live one.
    let code = rejection_code(
        ask(
            &entity,
            AgentEntityCommand::Instantiate {
                operation_id: operation(AgentOperationKind::DefinitionUpdate, "rogue"),
                definition: Box::new(definition()),
                settings: Box::new(AgentSettings::default()),
                provenance: Box::new(provenance(3)),
            },
        )
        .await,
    );
    assert_eq!(code, "agent-already-instantiated");

    system.shutdown();
}

#[test]
fn the_command_protocol_is_serializable() {
    // Standing constraint: every sharded entity command and reply must cross
    // `rakka-remote` from the first commit. No `Arc` payloads, no in-process reply
    // channels in the wire types.
    let command = instantiate_command();
    let encoded = serde_json::to_vec(&command).expect("the command should serialize");
    let decoded: AgentEntityCommand =
        serde_json::from_slice(&encoded).expect("the command should deserialize");
    assert_eq!(decoded, command);

    for command in [
        AgentEntityCommand::UpdateSettings {
            operation_id: operation(AgentOperationKind::SettingsUpdate, "2"),
            expected_revision: AgentRevisionNumber::INITIAL,
            changes: vec![AgentSettingsChange::RetrievalLimit(4)],
            provenance: Box::new(provenance(2)),
        },
        AgentEntityCommand::Suspend {
            operation_id: operation(AgentOperationKind::LifecycleCommand, "suspend-1"),
            expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
            provenance: Box::new(provenance(3)),
        },
        AgentEntityCommand::Describe,
    ] {
        let encoded = serde_json::to_vec(&command).expect("the command should serialize");
        let decoded: AgentEntityCommand =
            serde_json::from_slice(&encoded).expect("the command should deserialize");
        assert_eq!(decoded, command);
    }

    let reply = AgentEntityReply::Applied {
        outcome: rakka_agent::AgentEntityOutcome {
            status: AgentLifecycleStatus::Active,
            lifecycle_revision: AgentRevisionNumber::INITIAL,
            definition_revision: AgentRevisionNumber::INITIAL,
            settings_revision: AgentRevisionNumber::INITIAL,
        },
    };
    let encoded = serde_json::to_vec(&reply).expect("the reply should serialize");
    let decoded: AgentEntityReply =
        serde_json::from_slice(&encoded).expect("the reply should deserialize");
    assert_eq!(decoded, reply);
}

#[tokio::test]
async fn the_operation_log_stays_bounded() {
    let Fixture {
        system,
        store,
        entity,
        ..
    } = sharded_agent("AgentEntityBoundedLog");

    applied(ask(&entity, instantiate_command()).await);

    // A suspend, a resume, and a second suspend. The resume ages out of the
    // deduplication window below, and replaying it must not lift the suspension
    // that came after it.
    let resume = AgentEntityCommand::Resume {
        operation_id: operation(AgentOperationKind::LifecycleCommand, "resume-1"),
        expected_lifecycle_revision: AgentRevisionNumber::new(2),
        provenance: Box::new(provenance(3)),
    };
    applied(
        ask(
            &entity,
            AgentEntityCommand::Suspend {
                operation_id: operation(AgentOperationKind::LifecycleCommand, "suspend-1"),
                expected_lifecycle_revision: AgentRevisionNumber::INITIAL,
                provenance: Box::new(provenance(2)),
            },
        )
        .await,
    );
    applied(ask(&entity, resume.clone()).await);
    applied(
        ask(
            &entity,
            AgentEntityCommand::Suspend {
                operation_id: operation(AgentOperationKind::LifecycleCommand, "suspend-2"),
                expected_lifecycle_revision: AgentRevisionNumber::new(3),
                provenance: Box::new(provenance(4)),
            },
        )
        .await,
    );

    // Durable state must stay bounded, so the deduplication window is a ring.
    // These updates overflow it: the instantiation and every lifecycle operation
    // above age out of the window.
    for index in 0..AGENT_ENTITY_OPERATION_LOG_CAPACITY {
        let expected = AgentRevisionNumber::new(index as u64 + 1);
        applied(
            ask(
                &entity,
                AgentEntityCommand::UpdateSettings {
                    operation_id: operation(
                        AgentOperationKind::SettingsUpdate,
                        &format!("{index}"),
                    ),
                    expected_revision: expected,
                    changes: vec![AgentSettingsChange::RetrievalLimit(index as u32)],
                    provenance: Box::new(provenance(index as u64 + 2)),
                },
            )
            .await,
        );
    }

    let durable = load_agent_entity_state(&store, &scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the durable state should load")
        .expect("the agent was instantiated");
    assert_eq!(
        durable.applied_operations().len(),
        AGENT_ENTITY_OPERATION_LOG_CAPACITY
    );

    // The oldest operation has aged out of the window, but replaying it is still
    // safe: the settings update carries the revision it expects to succeed, and
    // that revision is long gone.
    let code = rejection_code(
        ask(
            &entity,
            AgentEntityCommand::UpdateSettings {
                operation_id: operation(AgentOperationKind::DefinitionUpdate, "1"),
                expected_revision: AgentRevisionNumber::INITIAL,
                changes: vec![AgentSettingsChange::RetrievalLimit(0)],
                provenance: Box::new(provenance(999)),
            },
        )
        .await,
    );
    assert_eq!(code, "stale-settings-revision");

    // The resume has aged out of the window too. Un-deduplicated, its replay
    // would reactivate an agent a later command suspended; the lifecycle fence
    // rejects it instead, and the suspension holds.
    let code = rejection_code(ask(&entity, resume).await);
    assert_eq!(code, "stale-lifecycle-revision");

    let durable = load_agent_entity_state(&store, &scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the durable state should load")
        .expect("the agent was instantiated");
    assert_eq!(durable.status(), AgentLifecycleStatus::Suspended);
    assert!(!durable.is_dispatch_permitted());

    system.shutdown();
}
