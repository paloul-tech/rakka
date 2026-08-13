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
