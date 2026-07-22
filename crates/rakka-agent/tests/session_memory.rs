//! Session memory and immutable context snapshots, driven through the run entity.
//!
//! Specification: sections 13.1, 13.2, and 13.5; scenarios 14, 16, and 17 of
//! section 18. A run wired with a session-memory backend must, as it cranks its
//! durable loop:
//!
//! - isolate its short-term memory by both agent id and run id (scenario 14);
//! - append each recorded turn idempotently, so a re-driven flush after a restart
//!   never duplicates an entry (scenario 16); and
//! - persist an immutable context snapshot before every model effect, so a
//!   re-driven settle — a dispatcher retry, a recovery — reuses the original
//!   rather than re-assembling from newer memory (scenario 17).
//!
//! The store-level halves of the same three scenarios live in the `memory` unit
//! tests; these drive the real run entity end to end.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    AgentContextSnapshotRef, AgentModelTurn, AgentModelUsage, AgentRunId, AgentRunMemory,
    AgentRunScope, AgentRunStatus, AgentTaskContent, AgentToolCallId, AgentToolCallRequest,
    AgentToolId, ContextSnapshotStore, InMemoryContextSnapshotStore, InMemorySessionMemoryStore,
    MemoryClassification, MemoryEntryId, MemoryEntryRole, MemoryOperationId, MemorySequence,
    SessionMemoryCursor, SessionMemoryEntry, SessionMemoryStore,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::AgentTimestampMillis;

mod common;

use common::*;

fn text_turn(text: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(text)
        .with_usage(AgentModelUsage {
            input_tokens: 8,
            output_tokens: 4,
            cost_micros: 2,
        })
}

fn tool_calling_turn(tool: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look that up.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id"),
                AgentToolId::new(tool).expect("tool id"),
                serde_json::json!({ "query": "ticket" }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
        .with_usage(AgentModelUsage {
            input_tokens: 10,
            output_tokens: 5,
            cost_micros: 3,
        })
}

fn stores() -> (
    Arc<InMemorySessionMemoryStore>,
    Arc<InMemoryContextSnapshotStore>,
) {
    (
        Arc::new(InMemorySessionMemoryStore::new()),
        Arc::new(InMemoryContextSnapshotStore::new()),
    )
}

/// A run that calls a tool on its first turn and proposes on its second records
/// every turn to session memory, isolated to its own scope, and persists one
/// immutable snapshot per model effect — and a re-driven loop never duplicates an
/// entry (scenarios 14 and 16).
#[tokio::test]
async fn a_run_records_its_turns_to_isolated_session_memory() {
    let (session, snapshots) = stores();
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    );

    let fx = Fixture::new(dispatcher)
        .with_memory(AgentRunMemory::new(session.clone(), snapshots.clone()));
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump().await.expect("the loop should run to completion");

    // The run recorded its opening input and exactly its two turns: the task's
    // input, turn one's assistant message and its tool result, and turn two's
    // assistant message. A re-driven flush across the recovery restarts `pump`
    // simulates never duplicated an entry.
    let scope = run_scope();
    let page = session
        .read(&scope, SessionMemoryCursor::start())
        .await
        .expect("read the session");
    assert_eq!(
        page.entries.len(),
        4,
        "the input, one assistant, one tool, one assistant"
    );
    let roles: Vec<MemoryEntryRole> = page.entries.iter().map(|entry| entry.role).collect();
    assert_eq!(
        roles,
        vec![
            MemoryEntryRole::User,
            MemoryEntryRole::Assistant,
            MemoryEntryRole::ToolResult,
            MemoryEntryRole::Assistant,
        ]
    );
    // The opening entry is the task's input, verbatim.
    assert_eq!(
        page.entries[0].content,
        AgentTaskContent::inline(serde_json::json!({ "ticket": 1 })).expect("the fixture input"),
        "the session opens with the task's bounded input"
    );
    // The sequence is monotonic and dense.
    let sequences: Vec<u64> = page
        .entries
        .iter()
        .map(|entry| entry.sequence.get())
        .collect();
    assert_eq!(sequences, vec![1, 2, 3, 4]);

    // Isolation: another run of the same agent, and a run of another agent, share
    // nothing with this run's session (scenario 14).
    let other_run = AgentRunScope::new(
        tenant(),
        agent_id(),
        AgentRunId::new("run-2").expect("run id"),
    )
    .expect("scope");
    assert!(session.is_empty(&other_run), "a sibling run sees nothing");

    // One immutable snapshot was persisted per model effect (turns one and two).
    assert_eq!(snapshots.len(&scope), 2, "one snapshot per model effect");
    let turn_two = snapshots
        .load(
            &scope,
            &AgentContextSnapshotRef::for_turn(&scope, 2).expect("ref"),
        )
        .await
        .expect("load")
        .expect("turn two snapshot exists");
    // Turn two's snapshot carries turn one's session, and it is untrusted context.
    assert!(
        !turn_two.session.is_empty(),
        "turn two's snapshot includes turn one's recorded session"
    );
    assert!(turn_two.is_untrusted());
    assert_eq!(turn_two.content_digest, turn_two.compute_digest());
}

/// A model effect's snapshot is persisted before the effect and is immutable: a
/// re-driven settle after newer memory has been written reuses the original
/// snapshot rather than assembling a fresh one (scenario 17).
#[tokio::test]
async fn a_model_effect_retry_uses_the_original_context_snapshot() {
    let (session, snapshots) = stores();
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            // Turn one is a plain text turn with no proposal, so the loop records
            // it and iterates to turn two, whose snapshot must then see turn one.
            .with_turn_for(1, text_turn("thinking"))
            .with_turn_for(2, proposing_turn("resolved")),
    );

    let fx = Fixture::new(dispatcher)
        .with_memory(AgentRunMemory::new(session.clone(), snapshots.clone()));
    fx.instantiate_agent().await;
    fx.create_task().await;
    let scope = run_scope();

    // Round one: crank to the turn-one model wait (its snapshot is persisted
    // from the session holding only the task's input), then answer the model
    // call.
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("crank to the turn-one wait");
    let turn_one = AgentContextSnapshotRef::for_turn(&scope, 1).expect("ref");
    let opening = snapshots
        .load(&scope, &turn_one)
        .await
        .expect("load")
        .expect("turn one snapshot exists");
    assert_eq!(
        opening
            .session
            .iter()
            .map(|entry| entry.role)
            .collect::<Vec<_>>(),
        vec![MemoryEntryRole::User],
        "turn one's snapshot carries exactly the task's input"
    );
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("answer the turn-one model call");

    // Round two: crank turn one's record into session memory, flush it, assemble
    // turn two's snapshot from it, and dispatch turn two's model effect — but do
    // not answer it yet.
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("crank to the turn-two wait");

    let turn_two = AgentContextSnapshotRef::for_turn(&scope, 2).expect("ref");
    let original = snapshots
        .load(&scope, &turn_two)
        .await
        .expect("load")
        .expect("turn two snapshot exists");
    assert!(
        !original.session.is_empty(),
        "turn two's snapshot saw turn one's flushed session"
    );

    // Newer memory arrives concurrently, after the snapshot was assembled.
    let concurrent = SessionMemoryEntry::new(
        MemoryEntryId::derive(&scope, "concurrent").expect("entry id"),
        MemoryOperationId::derive(&scope, "concurrent").expect("op id"),
        MemorySequence::new(99),
        MemoryEntryRole::User,
        AgentTaskContent::inline(serde_json::json!({ "late": true })).expect("content"),
        2,
        None,
        MemoryClassification::Unclassified,
        AgentTimestampMillis::new(999),
    )
    .expect("the entry is bounded");
    session
        .append(&scope, &concurrent)
        .await
        .expect("append newer memory");

    // A re-driven settle — a recovery, a dispatcher retry — must reuse the
    // original snapshot, not re-assemble it from the newer memory.
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("recover");
    run.settle_side_effects(&fx.router, now)
        .await
        .expect("re-drive the turn-two wait");
    let reused = snapshots
        .load(&scope, &turn_two)
        .await
        .expect("load")
        .expect("turn two snapshot still exists");
    assert_eq!(
        reused, original,
        "the re-driven settle reused the original snapshot despite newer memory"
    );
    // Exactly two snapshots exist: newer memory did not mint a third.
    assert_eq!(snapshots.len(&scope), 2);
}

/// Scenarios 14, 16, and 17 under the owner-kill sweep: kill the run's owner
/// at every durable write of the two-turn tool flow, on both sides of the
/// compare-and-set. However the owner died, the converged session holds
/// exactly one copy of each entry in dense sequence (a re-driven flush never
/// duplicated an append), exactly one immutable snapshot exists per model
/// effect, and a sibling run's scope still sees nothing. The run store is the
/// only store this flow's crash windows live in; the driver is the in-process
/// dispatcher, so owner kill at every write is the complete boundary set here.
#[tokio::test]
async fn session_memory_survives_any_owner_loss_without_duplicate_appends() {
    let build = || {
        let (session, snapshots) = stores();
        let dispatcher = ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn_for(1, tool_calling_turn("lookup"))
                .with_turn_for(2, proposing_turn("resolved")),
        )
        .with_tool_result(
            "lookup",
            AgentTaskContent::inline(serde_json::json!({ "found": true }))
                .expect("the tool result is inline-bounded"),
        );
        let fx = Fixture::new(dispatcher)
            .with_memory(AgentRunMemory::new(session.clone(), snapshots.clone()));
        (fx, session, snapshots)
    };

    let (reference, _session, _snapshots) = build();
    reference.instantiate_agent().await;
    reference.runs.reset_writes();
    reference.create_task().await;
    reference
        .pump()
        .await
        .expect("the reference flow completes");
    let writes = reference.runs.writes();
    assert!(
        writes >= 6,
        "the two-turn memory flow should make several durable writes, saw {writes}"
    );

    rakka_agent::testkit::sweep_crash_points(writes, |nth, point| async move {
        let (fx, session, snapshots) = build();
        fx.instantiate_agent().await;

        fx.runs.crash_at(nth, point);
        fx.create_task().await;
        let _crashed = fx.pump().await;

        // A new owner activates and finds only what was durably committed.
        fx.runs.survive();
        fx.pump().await.unwrap_or_else(|error| {
            panic!("crash {point:?} at write {nth} did not converge: {error}")
        });

        let run = fx.run_snapshot().await.expect("the run exists");
        assert_eq!(
            run.status,
            AgentRunStatus::Completed,
            "crash {point:?} at write {nth} should still complete"
        );

        let scope = run_scope();
        let page = session
            .read(&scope, SessionMemoryCursor::start())
            .await
            .expect("read the session");
        let roles: Vec<MemoryEntryRole> = page.entries.iter().map(|entry| entry.role).collect();
        assert_eq!(
            roles,
            vec![
                MemoryEntryRole::User,
                MemoryEntryRole::Assistant,
                MemoryEntryRole::ToolResult,
                MemoryEntryRole::Assistant,
            ],
            "crash {point:?} at write {nth} duplicated or dropped a session entry"
        );
        let sequences: Vec<u64> = page
            .entries
            .iter()
            .map(|entry| entry.sequence.get())
            .collect();
        assert_eq!(
            sequences,
            vec![1, 2, 3, 4],
            "crash {point:?} at write {nth} broke the dense session sequence"
        );

        // Isolation held through every recovery (scenario 14).
        let other_run = AgentRunScope::new(
            tenant(),
            agent_id(),
            AgentRunId::new("run-2").expect("run id"),
        )
        .expect("scope");
        assert!(
            session.is_empty(&other_run),
            "crash {point:?} at write {nth} leaked into a sibling run's session"
        );

        // One immutable snapshot per model effect — a re-driven settle reused
        // the original rather than minting another (scenario 17).
        assert_eq!(
            snapshots.len(&scope),
            2,
            "crash {point:?} at write {nth} minted a duplicate context snapshot"
        );
    })
    .await;
}
