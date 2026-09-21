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

/// The send leaves' futures stay small enough for a test thread's stack.
///
/// `send` awaits `send_message_normalized`, `team_command_normalized`, or
/// `conversation_command_normalized`, and each of those awaits the entity
/// stores; the whole nest is one flattened state machine, so anything a leaf
/// binds across an await is reserved in `send`'s own future — in every
/// branch, whether or not that branch runs. Slice 7.2's first ingress commit
/// bound an owned `(SendMessageRequest, NormalizedAgentCommand)` at five such
/// sites and pushed the debug-build future past the 2 MiB a Rust test thread
/// gets, aborting an acceptance example with `fatal runtime error: stack
/// overflow` and no other symptom. Boxing the admitted pair is what keeps it
/// down; this is what keeps the next large binding from undoing that
/// silently.
#[test]
fn the_send_future_stays_within_a_test_threads_stack() {
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
    let future = service.send(&params, &request);
    let bytes = std::mem::size_of_val(&future);
    drop(future);

    // The bound has to sit near the real measurement to mean anything: the
    // regression it exists for was 22_008 bytes, against 17_408 before it and
    // 18_240 after the fix. 20 KiB leaves about a tenth of headroom over the
    // current size and still fails at the size that aborted the acceptance
    // example. A deliberate, measured increase may raise it — but only after
    // checking that
    // `cargo test -p rakka-example-coordination-capability-acceptance` still
    // runs, because that is the symptom this number stands in for.
    const CEILING: usize = 20 * 1024;
    assert!(
        bytes <= CEILING,
        "`send`'s future is {bytes} bytes, over the {CEILING}-byte ceiling. \
         Something large is now held across an await in one of the send \
         leaves — box it (as the admitted ingress pair is boxed) rather than \
         raising this bound; at 22_008 bytes this overflowed a test thread's \
         stack with no symptom but a SIGABRT."
    );
    println!("send future: {bytes} bytes");
}
