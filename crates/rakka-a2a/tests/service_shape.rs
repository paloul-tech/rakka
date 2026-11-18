//! Shape rules the A2A agent service has to keep, enforced against its own
//! source.
//!
//! Some invariants are not expressible in the type system but are cheap to
//! state about the code. This one has cost three slices of silent metrics to
//! learn: the service builds its own entity stores rather than routing
//! through the sharded entities, the store constructors default to a noop
//! recorder, and a store built without the service's recorder produces no
//! error, no log, and no symptom other than a counter that never leaves zero.

#![cfg(feature = "agents")]

use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("the rakka-a2a manifest should live under crates/rakka-a2a")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

/// Every entity store the service builds is built by the one accessor that
/// wires the recorder into it.
///
/// The accessors are what make an unwired store unreachable; this is what
/// makes the accessors unavoidable. A second `::new` call site would compile,
/// pass every existing test, and silently stop recording — which is exactly
/// how `rakka.agent.moderation.turns` (slice 5.3), then
/// `rakka.agent.human.results` and `rakka.agent.team.operations` (slice 5.4)
/// each shipped at zero.
#[test]
fn the_service_builds_every_entity_store_through_its_wired_accessor() {
    let service = read("crates/rakka-a2a/src/agents/service.rs");

    for (store, accessor) in [
        ("AgentTaskEntityStore::new(", "fn task_store("),
        ("AgentTeamEntityStore::new(", "fn team_store("),
        (
            "AgentConversationEntityStore::new(",
            "fn conversation_store(",
        ),
    ] {
        assert!(
            service.contains(accessor),
            "the service should keep its `{accessor}` accessor: it is the only place \
             `{store}` may be called, and the only place the recorder is wired in"
        );
        let sites = service.matches(store).count();
        assert_eq!(
            sites, 1,
            "`{store}` should appear exactly once in service.rs — inside `{accessor}` — \
             but appears {sites} times. Build the store through the accessor instead; a \
             direct call records through the noop recorder and its counters stay at zero \
             with no other symptom."
        );
    }

    // The accessors are only worth anything if they actually wire the
    // recorder, so pin that too: three constructions, three wirings.
    let wired = service
        .matches(".with_metrics(self.metrics.clone())")
        .count();
    assert_eq!(
        wired, 3,
        "each of the three store accessors should wire the service's recorder"
    );
}

/// The request entries box what they send.
///
/// `send` and `send_message` are the doors a request comes through, and
/// each boxes the future that does the work. From here their futures are
/// a few machine words: the arguments and the box. That is the point: a
/// caller holds a future it awaits inline, and in a debug build keeps
/// stack for it at each call, so an unboxed request future of 9 KiB cost
/// every handler, and every dispatch pass whose handoff calls this service
/// in-process, that much for each send (issue #162).
///
/// This test fails if a door stops boxing. What the box holds is private,
/// and its size is held inside the crate, by
/// `the_send_futures_stay_near_their_measured_size` in
/// `src/agents/service.rs`. Before the doors boxed, the test in this place
/// measured `send`'s own future against a 20 KiB ceiling; left as it was,
/// it would have measured the box and passed whatever the box held.
#[test]
fn the_send_entries_box_what_they_send() {
    use std::sync::Arc;

    use rakka_a2a::agents::{A2AStaticAgentCatalog, RakkaAgentA2AService};
    use rakka_a2a::auth::AllowAllAuthorizer;
    use rakka_a2a::mapping::A2AHeaderTenantResolver;
    use rakka_a2a::projection::InMemoryA2ATaskProjectionStore;
    use rakka_agent::{
        AgentConversationState, AgentEntityState, AgentExchangeRouter, AgentRunState,
        AgentTaskState, AgentTeamState, InMemoryAgentConversationHistoryStore,
        InMemoryAgentTaskHistoryStore, InMemoryAgentTeamHistoryStore,
    };
    use rakka_persistence::InMemoryDurableStateStore;

    let service = RakkaAgentA2AService::new(
        InMemoryDurableStateStore::<AgentTaskState>::default(),
        InMemoryDurableStateStore::<AgentEntityState>::default(),
        InMemoryAgentTaskHistoryStore::new(),
        InMemoryDurableStateStore::<AgentRunState>::default(),
        InMemoryDurableStateStore::<AgentTeamState>::default(),
        InMemoryAgentTeamHistoryStore::new(),
        InMemoryDurableStateStore::<AgentConversationState>::default(),
        InMemoryAgentConversationHistoryStore::new(),
        AgentExchangeRouter::new(),
        Arc::new(A2AStaticAgentCatalog::new()),
        Arc::new(InMemoryA2ATaskProjectionStore::local()),
        Arc::new(A2AHeaderTenantResolver),
        Arc::new(AllowAllAuthorizer),
    );
    let params = a2a_server::ServiceParams::new();
    let request = a2a::SendMessageRequest {
        message: a2a::Message::new(a2a::Role::User, Vec::new()),
        configuration: None,
        metadata: None,
        tenant: None,
    };

    // Constructed, never polled: the size is the state machine's, not a run's.
    let send = service.send(&params, &request);
    let send_bytes = std::mem::size_of_val(&send);
    drop(send);
    let send_message = service.send_message(&params, &request);
    let send_message_bytes = std::mem::size_of_val(&send_message);
    drop(send_message);
    println!("send: {send_bytes} bytes, send_message: {send_message_bytes} bytes");

    // Both measure 40 bytes. An entry that awaits its work in place measures
    // what the work holds, which is over 9,000.
    const A_DOOR: usize = 256;
    for (entry, bytes) in [("send", send_bytes), ("send_message", send_message_bytes)] {
        assert!(
            bytes <= A_DOOR,
            "`{entry}`'s future is {bytes} bytes: the entry no longer boxes \
             the future that does its work, so every caller holds that \
             future inline. Box it at the entry \
             (`Box::pin(self.{entry}_unboxed(..)).await`)."
        );
    }
}
