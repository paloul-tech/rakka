//! A terminal run's short-term retention, discharged from durable state.
//!
//! Specification: sections 13.1, 13.2, and 13.5; open decision 7.
//!
//! The stores have held `purge_run` since slice 2.1 and nothing called it: a
//! grep for `purge_run`, `purge_expired`, `tombstone`, and `delete` on the
//! memory stores finds only their own unit tests. So the *pieces* were
//! proven and the *composition* was not — deciding a run is terminal, knowing
//! when it became terminal, purging both tiers in an order a crash between
//! them survives, and staying idempotent under re-drive.
//!
//! The terminal timestamp is the part that could not be inferred. A retention
//! deadline measured from `AgentRunState::updated_at` would recede every time
//! a settlement or return command landed on an already-terminal run, so the
//! run stamps `terminal_at` once, under the guard that makes `terminate`
//! once-only.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    discharge_run_memory_retention, AgentContextSnapshotRef, AgentMemoryRetentionSweep,
    AgentModelTurn, AgentPrivateMemory, AgentPrivateMemoryId, AgentPrivateMemoryKind,
    AgentPrivateMemoryStore, AgentRecordKind, AgentRunMemory, AgentRunRetentionOutcome,
    AgentRunScope, AgentRunStatus, AgentSchemaCompatibility, AgentSchemaPolicy, AgentScope,
    AgentTaskContent, ContextSnapshotStore, InMemoryAgentPrivateMemoryStore,
    InMemoryContextSnapshotStore, InMemorySessionMemoryStore, MemoryClassification,
    MemoryOperationId, MemoryTombstoneReason, PrivateMemoryExpectation,
    PrivateMemoryTombstoneRequest, SessionMemoryCursor, SessionMemoryStore, SessionPurgeOutcome,
    SessionRetentionPolicy, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
    CURRENT_AGENT_RUN_STATE_SCHEMA_VERSION,
};
use rakka_agent_workflow::{AgentTimestampMillis, StateSchemaVersion};
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

/// A retention window short enough that a test clock can pass it.
const WINDOW_MS: u64 = 1_000;

fn text_turn(text: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text(text)
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "resolved" }))
                .expect("the proposal is inline-bounded"),
        )
}

struct World {
    fx: Fixture,
    memory: AgentRunMemory,
    session: Arc<InMemorySessionMemoryStore>,
    snapshots: Arc<InMemoryContextSnapshotStore>,
    private: Arc<InMemoryAgentPrivateMemoryStore>,
}

fn world() -> World {
    let session = Arc::new(InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(InMemoryContextSnapshotStore::new());
    let private = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    let memory =
        AgentRunMemory::new(session.clone(), snapshots.clone()).with_private_store(private.clone());
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("checking the ticket history"))
            .with_turn_for(2, proposing_turn()),
    );
    let fx = Fixture::new(dispatcher).with_memory(memory.clone());
    World {
        fx,
        memory,
        session,
        snapshots,
        private,
    }
}

impl World {
    async fn run_to_completion(&self) {
        self.fx.instantiate_agent().await;
        self.fx.create_task().await;
        self.fx.pump().await.expect("the loop runs to completion");
    }

    async fn discharge(
        &self,
        policy: &SessionRetentionPolicy,
        now: u64,
    ) -> AgentRunRetentionOutcome {
        discharge_run_memory_retention(
            &self.fx.runs,
            &self.memory,
            &run_scope(),
            policy,
            &AgentSchemaPolicy::default(),
            AgentTimestampMillis::new(now),
        )
        .await
        .expect("the discharge completes")
    }

    async fn session_entries(&self) -> usize {
        self.session
            .read(&run_scope(), SessionMemoryCursor::start())
            .await
            .expect("the session reads")
            .entries
            .len()
    }

    async fn snapshot_exists(&self, turn: u64) -> bool {
        let scope = run_scope();
        self.snapshots
            .load(
                &scope,
                &AgentContextSnapshotRef::for_turn(&scope, turn).expect("reference"),
            )
            .await
            .expect("the snapshot store reads")
            .is_some()
    }

    /// The schema version the run's persisted record carries, read from the
    /// serialized record rather than from a constant, so a stamp the code
    /// never wrote cannot be asserted into existence.
    async fn run_schema_version(&self) -> u64 {
        let state = rakka_agent::load_agent_run_state(
            &self.fx.runs,
            &run_scope(),
            &AgentSchemaPolicy::default(),
        )
        .await
        .expect("the run state loads")
        .expect("the run exists");
        serde_json::to_value(&state).expect("the run state serializes")["schema_version"]
            .as_u64()
            .expect("the record carries a numeric schema version")
    }

    /// Rewrites the persisted run record's schema version, the way a peer
    /// binary from before a bump would have written it.
    async fn rewrite_run_schema_version(&self, version: u64) {
        let id = run_scope().persistence_id();
        let record = self
            .fx
            .runs
            .load(&id)
            .await
            .expect("the run record loads")
            .expect("the run record exists");
        let mut value = serde_json::to_value(&record.state).expect("the run state serializes");
        value["schema_version"] = serde_json::json!(version);
        let downgraded = serde_json::from_value(value).expect("the tampered record deserializes");
        self.fx
            .runs
            .compare_and_set(&id, record.revision, downgraded)
            .await
            .expect("the tampered record persists");
    }

    async fn terminal_at(&self) -> Option<AgentTimestampMillis> {
        rakka_agent::load_agent_run_state(
            &self.fx.runs,
            &run_scope(),
            &AgentSchemaPolicy::default(),
        )
        .await
        .expect("the run state loads")
        .and_then(|state| state.run().and_then(|run| run.terminal_at))
    }
}

fn window(millis: u64) -> SessionRetentionPolicy {
    SessionRetentionPolicy::bounded_default().with_retain_for_millis(millis)
}

fn private_memory(scope: &AgentScope, name: &str, text: &str) -> AgentPrivateMemory {
    AgentPrivateMemory::new(
        AgentPrivateMemoryId::new(format!("mem-{name}")).expect("memory id"),
        MemoryOperationId::derive_for_agent(scope, format!("create-{name}")).expect("op id"),
        AgentPrivateMemoryKind::Semantic,
        AgentTaskContent::inline(serde_json::json!(text)).expect("content"),
        9_000,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(1),
    )
    .expect("the memory is bounded")
}

// ---------------------------------------------------------------------------
// The gate: only a terminal run, and only once its window has elapsed.
// ---------------------------------------------------------------------------

/// A live run refuses discharge and loses nothing.
#[tokio::test]
async fn a_live_run_refuses_discharge_and_deletes_nothing() {
    let world = world();
    world.fx.instantiate_agent().await;
    world.fx.create_task().await;
    // One settle: the run exists and has recorded a turn, but has not
    // proposed a result.
    world
        .fx
        .settle_task_at(&task_scope())
        .await
        .expect("the task settles");

    let outcome = world.discharge(&window(WINDOW_MS), 1_000_000).await;
    assert!(
        matches!(outcome, AgentRunRetentionOutcome::NotTerminal { .. }),
        "a live run must not be purgeable at any clock: {outcome:?}"
    );
}

/// A terminal run retains until its window elapses, then purges.
#[tokio::test]
async fn a_terminal_run_retains_until_its_window_elapses() {
    let world = world();
    world.run_to_completion().await;
    let terminal_at = world
        .terminal_at()
        .await
        .expect("a completed run stamps its terminal time")
        .as_millis();
    assert!(world.session_entries().await > 0, "the run recorded turns");

    let early = world
        .discharge(&window(WINDOW_MS), terminal_at + WINDOW_MS - 1)
        .await;
    assert_eq!(
        early,
        AgentRunRetentionOutcome::Discharged {
            snapshots: SessionPurgeOutcome::NotYetDue,
            session: SessionPurgeOutcome::NotYetDue,
        },
        "the window had not elapsed"
    );
    assert!(world.session_entries().await > 0, "nothing was deleted yet");

    let due = world
        .discharge(&window(WINDOW_MS), terminal_at + WINDOW_MS)
        .await;
    assert!(
        due.deleted_anything(),
        "the window elapsed and nothing was deleted: {due:?}"
    );
    assert_eq!(world.session_entries().await, 0);
    assert!(!world.snapshot_exists(1).await);
}

/// A legal hold survives every pass, however long the window.
#[tokio::test]
async fn a_legal_hold_survives_every_pass() {
    let world = world();
    world.run_to_completion().await;
    let held = window(0).with_legal_hold(true);

    for round in 0..3 {
        let outcome = world.discharge(&held, 1_000_000).await;
        assert_eq!(
            outcome,
            AgentRunRetentionOutcome::Discharged {
                snapshots: SessionPurgeOutcome::Held,
                session: SessionPurgeOutcome::Held,
            },
            "round {round} did not honour the hold"
        );
    }
    assert!(world.session_entries().await > 0);
    assert!(world.snapshot_exists(1).await);
}

/// A due run purges both tiers, and a replay purges zero.
#[tokio::test]
async fn a_due_terminal_run_purges_both_tiers_and_a_replay_purges_zero() {
    let world = world();
    world.run_to_completion().await;

    let first = world.discharge(&window(0), 1_000_000).await;
    let AgentRunRetentionOutcome::Discharged { snapshots, session } = first else {
        panic!("the discharge was refused: {first:?}");
    };
    assert!(matches!(snapshots, SessionPurgeOutcome::Purged { entries } if entries > 0));
    assert!(matches!(session, SessionPurgeOutcome::Purged { entries } if entries > 0));
    assert!(first.deleted_anything(), "records were removed: {first:?}");

    let replay = world.discharge(&window(0), 1_000_001).await;
    assert_eq!(
        replay,
        AgentRunRetentionOutcome::Discharged {
            snapshots: SessionPurgeOutcome::Purged { entries: 0 },
            session: SessionPurgeOutcome::Purged { entries: 0 },
        },
        "a replayed discharge must delete nothing and say so"
    );
    // The predicate must agree with the payload it summarizes. A caller that
    // gates a deletion audit entry or an erasure notification on it would
    // otherwise record one on every idempotent re-drive of the same run.
    assert!(
        !replay.deleted_anything(),
        "a replay that deleted nothing must not report a deletion: {replay:?}"
    );
}

// ---------------------------------------------------------------------------
// The terminal stamp is written once and does not move.
// ---------------------------------------------------------------------------

/// A re-driven wind-down does not push the retention deadline forward.
///
/// This is the whole reason `terminal_at` exists rather than reusing
/// `updated_at`: a terminal run keeps accepting settlement and return
/// commands, each of which advances `updated_at`.
#[tokio::test]
async fn the_terminal_stamp_is_written_once_and_does_not_move() {
    let world = world();
    world.run_to_completion().await;
    let first = world.terminal_at().await.expect("stamped");

    // Keep settling: the run is terminal, and its settlement exchanges are
    // still draining. Each accepted transition advances `updated_at`.
    for _round in 0..3 {
        world
            .fx
            .settle_task_at(&task_scope())
            .await
            .expect("the task settles");
    }
    let after = world.terminal_at().await.expect("still stamped");

    assert_eq!(
        first, after,
        "the retention clock moved when a terminal run kept settling"
    );

    let state = rakka_agent::load_agent_run_state(
        &world.fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
    )
    .await
    .expect("the run state loads")
    .expect("the run exists");
    assert!(
        state.updated_at().as_millis() >= first.as_millis(),
        "the test is vacuous unless updated_at really is the later clock"
    );
    assert_eq!(
        state.run().expect("the run exists").status,
        AgentRunStatus::Completed
    );
}

// ---------------------------------------------------------------------------
// Cross-tier: what a run discharge does and does not reach.
// ---------------------------------------------------------------------------

/// The agent's private memory outlives the run whose retention was discharged.
#[tokio::test]
async fn a_run_discharge_leaves_the_agents_private_memory_intact() {
    let world = world();
    let scope = agent_scope();
    let kept = private_memory(&scope, "kept", "an agent-level fact");
    world
        .private
        .upsert(&scope, &kept, PrivateMemoryExpectation::Absent)
        .await
        .expect("the memory seeds");

    world.run_to_completion().await;
    let outcome = world.discharge(&window(0), 1_000_000).await;
    assert!(outcome.deleted_anything());

    let still_there = world
        .private
        .get(
            &scope,
            &kept.memory_id,
            AgentTimestampMillis::new(1_000_001),
        )
        .await
        .expect("the private store reads");
    assert!(
        still_there.is_some(),
        "a run's retention discharge deleted agent-level memory, which outlives it"
    );
}

/// A memory withdrawn after a snapshot embedded it survives in that snapshot.
///
/// Required, not an oversight: specification 13.5 makes a model-effect retry
/// read the *same* snapshot, so scrubbing embedded content on withdrawal
/// would make the immutable tier mutable and break scenario 17. The exposure
/// is real and bounded — the next test is the bound.
#[tokio::test]
async fn a_memory_tombstoned_after_a_snapshot_embedded_it_survives_in_that_snapshot() {
    let scope = agent_scope();
    let session = Arc::new(InMemorySessionMemoryStore::new());
    let snapshots = Arc::new(InMemoryContextSnapshotStore::new());
    let private = Arc::new(InMemoryAgentPrivateMemoryStore::new());
    let embedded = private_memory(&scope, "embedded", "ticket WITHDRAWN-LATER");
    private
        .upsert(&scope, &embedded, PrivateMemoryExpectation::Absent)
        .await
        .expect("the memory seeds");

    let memory = AgentRunMemory::new(session.clone(), snapshots.clone())
        .with_private_store(private.clone())
        .with_retrieval(rakka_agent::AgentMemoryRetrieval::new(
            Arc::new(rakka_agent::InMemoryPrivateMemoryRetriever::new(
                private.clone(),
            )),
            private.clone(),
            rakka_agent::AgentGuardrailChain::new(rakka_agent::AgentRevisionNumber::INITIAL),
        ));
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, text_turn("checking the ticket history"))
            .with_turn_for(2, proposing_turn()),
    );
    let fx = Fixture::new(dispatcher).with_memory(memory.clone());
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop runs to completion");

    let run = run_scope();
    let snapshot = snapshots
        .load(
            &run,
            &AgentContextSnapshotRef::for_turn(&run, 1).expect("reference"),
        )
        .await
        .expect("the snapshot store reads")
        .expect("the first turn persisted a snapshot");
    let encoded = serde_json::to_string(&snapshot).expect("the snapshot serializes");
    assert!(
        encoded.contains("WITHDRAWN-LATER"),
        "the snapshot never embedded the memory, so this proves nothing: {encoded}"
    );

    // The subject asks to be forgotten; the private record is withdrawn.
    private
        .tombstone(
            &scope,
            &PrivateMemoryTombstoneRequest {
                memory_id: embedded.memory_id.clone(),
                operation_id: MemoryOperationId::derive_for_agent(&scope, "withdraw")
                    .expect("op id"),
                reason: MemoryTombstoneReason::Retracted,
                tombstoned_at: AgentTimestampMillis::new(2),
            },
        )
        .await
        .expect("the withdrawal lands");

    let after = snapshots
        .load(
            &run,
            &AgentContextSnapshotRef::for_turn(&run, 1).expect("reference"),
        )
        .await
        .expect("the snapshot store reads")
        .expect("the snapshot is immutable");
    assert_eq!(
        serde_json::to_string(&after).expect("serializes"),
        encoded,
        "a private withdrawal changed an immutable snapshot"
    );

    // ...and the bound: the run's snapshot retention is what erases it.
    let outcome = discharge_run_memory_retention(
        &fx.runs,
        &memory,
        &run,
        &window(0),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(1_000_000),
    )
    .await
    .expect("the discharge completes");
    assert!(outcome.deleted_anything(), "{outcome:?}");
    assert!(
        snapshots
            .load(
                &run,
                &AgentContextSnapshotRef::for_turn(&run, 1).expect("reference"),
            )
            .await
            .expect("the snapshot store reads")
            .is_none(),
        "the embedded copy outlived the run's retention window"
    );
}

// ---------------------------------------------------------------------------
// The sweep reports what it skipped instead of aborting.
// ---------------------------------------------------------------------------

/// A sweep over a mixed set of scopes reports every disposition.
#[tokio::test]
async fn a_sweep_over_many_runs_reports_what_it_skipped_without_aborting() {
    let world = world();
    world.run_to_completion().await;

    let absent = AgentRunScope::new(
        tenant(),
        agent_id(),
        rakka_agent::AgentRunId::new("no-such-run").expect("run id"),
    )
    .expect("the scope is valid");

    let sweep = AgentMemoryRetentionSweep::new(world.fx.runs.clone(), world.memory.clone());
    let report = sweep
        .discharge(
            [run_scope(), absent.clone(), absent],
            &window(0),
            AgentTimestampMillis::new(1_000_000),
        )
        .await
        .expect("the sweep completes");

    assert_eq!(report.discharged, 1);
    assert_eq!(
        report.absent, 2,
        "an absent scope is reported, not an error that stops the pass"
    );
    assert!(report.records_deleted > 0);
}

// ---------------------------------------------------------------------------
// The sweep's refusal counters are per run, and the schema version fences the
// stamp against a peer that would erase it.
// ---------------------------------------------------------------------------

/// `held` and `not_yet_due` count runs, as their documentation says.
///
/// Counting them once per *tier* doubled every refusal: ten held scopes would
/// report twenty held against ten discharged, which reads as a report of
/// twenty runs the operator does not have.
#[tokio::test]
async fn a_sweep_counts_its_refusals_per_run_not_per_tier() {
    let world = world();
    world.run_to_completion().await;
    let terminal_at = world
        .terminal_at()
        .await
        .expect("a completed run stamps its terminal time")
        .as_millis();
    let sweep = AgentMemoryRetentionSweep::new(world.fx.runs.clone(), world.memory.clone());

    let held = sweep
        .discharge(
            [run_scope()],
            &window(0).with_legal_hold(true),
            AgentTimestampMillis::new(1_000_000),
        )
        .await
        .expect("the sweep completes");
    assert_eq!(held.discharged, 1);
    assert_eq!(
        held.held, 1,
        "one run held on both of its tiers is one held run, not two"
    );
    assert_eq!(held.records_deleted, 0);

    let early = sweep
        .discharge(
            [run_scope()],
            &window(WINDOW_MS),
            AgentTimestampMillis::new(terminal_at + WINDOW_MS - 1),
        )
        .await
        .expect("the sweep completes");
    assert_eq!(early.discharged, 1);
    assert_eq!(
        early.not_yet_due, 1,
        "one run short of its window on both tiers is one not-yet-due run"
    );
    assert_eq!(early.held, 0);
}

/// A run created before `terminal_at` existed upgrades its schema version at
/// the transition that gives it the field.
///
/// Without the upgrade the bump protects nothing that matters: a record
/// *created* by the older binary keeps saying version 1, so that binary keeps
/// reading it — and serde drops the field it does not know, on the next
/// settlement or return command it applies to the already-terminal run. The
/// stamp is written once under an already-terminal guard, so nothing could
/// ever put it back, and the run's session rows and snapshots would never be
/// purged.
#[tokio::test]
async fn a_pre_bump_run_record_upgrades_its_schema_when_it_terminalizes() {
    let world = world();
    world.fx.instantiate_agent().await;
    world.fx.create_task().await;

    // The record as the older binary wrote it: version 1, no stamp to carry.
    world.rewrite_run_schema_version(1).await;
    assert_eq!(world.run_schema_version().await, 1);
    assert_eq!(
        world.terminal_at().await,
        None,
        "the run is still live, so there is nothing to erase yet"
    );

    world.fx.pump().await.expect("the loop runs to completion");

    assert!(
        world.terminal_at().await.is_some(),
        "the completed run stamped its terminal time"
    );
    assert_eq!(
        world.run_schema_version().await,
        u64::from(CURRENT_AGENT_RUN_STATE_SCHEMA_VERSION.get()),
        "the record carries a field only the current version knows, and still \
         claims the version that does not"
    );

    // And that is what a peer from before the bump now meets on the load path.
    let older_binary = AgentSchemaPolicy::default().with_compatibility(
        AgentRecordKind::RunState,
        AgentSchemaCompatibility::n_plus_one(StateSchemaVersion::new(1)),
    );
    assert!(
        rakka_agent::load_agent_run_state(&world.fx.runs, &run_scope(), &older_binary)
            .await
            .is_err(),
        "a binary that cannot round-trip the stamp must fail closed, not load \
         the record and drop it"
    );
}
