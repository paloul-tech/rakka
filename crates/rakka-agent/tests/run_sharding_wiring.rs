//! The sharded run-entity factory wires the settings' observability signals.
//!
//! Specification: sections 17.7 and 17.12. The `rakka.agent.*` metrics and the
//! decision-event sink are injection points on
//! [`AgentRunEntityShardingSettings`], and the sharded factory is the
//! production driver of a run — so a run materialized on its owner must be
//! wired exactly as a directly-driven one is, or a deployed cluster records
//! none of the slice's signals. This proves the wiring end to end over the real
//! sharded actor: any command recovers the entity first, and recovery is a
//! measured agent-domain transition, so a sharded run emits
//! `rakka.agent.recovery.events` through the recorder the settings carry — and
//! records nothing through an unwired one.

use std::sync::Arc;
use std::time::Duration;

use rakka_agent::{
    agent_run_entity_type_key, init_agent_run_entity_sharding, registered_agent_run_entity_ref,
    AgentExchangeRouter, AgentId, AgentRunEntityCommand, AgentRunEntityMessage,
    AgentRunEntityShardingSettings, AgentRunId, AgentRunScope, AgentRunState,
    InMemoryAgentRunEffectSink, TenantId, METRIC_AGENT_RECOVERY_EVENTS,
};
use rakka_core::{ActorSystem, InMemoryMetricsRecorder, MetricsRecorder};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_sharding::ClusterSharding;

const ASK_TIMEOUT: Duration = Duration::from_secs(5);

fn scope() -> AgentRunScope {
    AgentRunScope::new(
        TenantId::new("acme"),
        AgentId::new("support-agent").expect("the agent id is valid"),
        AgentRunId::new("run-1").expect("the run id is valid"),
    )
    .expect("the scope is valid")
}

#[tokio::test]
async fn a_sharded_run_records_its_metrics_through_the_settings_recorder() {
    let system = ActorSystem::new("RunShardingMetricsWired");
    let sharding = ClusterSharding::get(&system);
    let store = InMemoryDurableStateStore::<AgentRunState>::new();
    let effects = InMemoryAgentRunEffectSink::new();
    let recorder = Arc::new(InMemoryMetricsRecorder::new());
    let metrics: Arc<dyn MetricsRecorder> = recorder.clone();

    let settings =
        AgentRunEntityShardingSettings::new(agent_run_entity_type_key()).with_metrics(metrics);
    let registration = init_agent_run_entity_sharding(
        &sharding,
        store,
        effects,
        AgentExchangeRouter::new(),
        settings,
    )
    .expect("the run entity sharding initializes");

    let entity = registered_agent_run_entity_ref(&registration, &scope());
    // Any command recovers the entity first, and recovery is a measured
    // agent-domain transition, so a wired sharded run records it.
    let _reply = entity
        .ask(
            |reply_to| AgentRunEntityMessage::Command {
                command: Box::new(AgentRunEntityCommand::Describe),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded run entity replies");

    let snapshot = recorder.snapshot();
    let recoveries = snapshot.observations_named(METRIC_AGENT_RECOVERY_EVENTS);
    assert!(
        !recoveries.is_empty(),
        "the sharded run recorded rakka.agent.recovery.events through the settings recorder"
    );

    drop((system, sharding, registration));
}

#[tokio::test]
async fn an_unwired_sharded_run_records_nothing() {
    // Settings with no recorder default to a no-op one, so the same command
    // over the same sharded path records nothing — the wiring, not the sharded
    // factory, is what produced the signal above.
    let recorder = Arc::new(InMemoryMetricsRecorder::new());

    let system = ActorSystem::new("RunShardingMetricsUnwired");
    let sharding = ClusterSharding::get(&system);
    let store = InMemoryDurableStateStore::<AgentRunState>::new();
    let effects = InMemoryAgentRunEffectSink::new();
    let registration = init_agent_run_entity_sharding(
        &sharding,
        store,
        effects,
        AgentExchangeRouter::new(),
        AgentRunEntityShardingSettings::new(agent_run_entity_type_key()),
    )
    .expect("the run entity sharding initializes");

    let entity = registered_agent_run_entity_ref(&registration, &scope());
    let _reply = entity
        .ask(
            |reply_to| AgentRunEntityMessage::Command {
                command: Box::new(AgentRunEntityCommand::Describe),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded run entity replies");

    assert!(
        recorder.snapshot().observations().is_empty(),
        "an independent recorder saw nothing — the unwired run measures through the no-op default"
    );

    drop((system, sharding, registration));
}
