//! When a checkpoint's resolution leaves its trace: on the call whose
//! transition committed it, exactly once, whatever the settle pass after it
//! did.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    load_agent_run_state, AgentApprovalDecision, AgentCheckpoint, AgentCheckpointDecision,
    AgentEffectPolicies, AgentEffectResolution, AgentEffectSpec, AgentModelTurn, AgentOperationId,
    AgentOperationKind, AgentReconciliationDecision, AgentRunEffect, AgentRunEffectOutcome,
    AgentRunEntityCommand, AgentRunEntityReply, AgentRunEntityStore, AgentRunError, AgentRunMemory,
    AgentRunScope, AgentRunState, AgentRunStatus, AgentSchemaPolicy, AgentSegmentOperation,
    AgentTaskContent, AgentTelemetrySegment, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    InMemoryAgentRunEffectSink, InMemoryAgentSegmentSink, InMemoryContextSnapshotStore,
    InMemorySessionMemoryStore, MemoryError, MemoryFuture, SessionMemoryCursor, SessionMemoryEntry,
    SessionMemoryPage, SessionMemoryStore, SessionPurgeOutcome, SessionRetentionPolicy,
    ATTR_AGENT_TELEMETRY_LINK_KIND, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
    LINK_KIND_PARKED_CHECKPOINT, LINK_KIND_RESUME_REQUEST,
};
use rakka_agent_workflow::{AgentTelemetryContext, AgentTimestampMillis, PrincipalRef};
use rakka_persistence::{
    DurableError, DurableStateStore, PersistenceId, Revision, StateRecord, StoreFuture,
};

mod common;

use common::*;

const INGRESS_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const REQUEST_PARENT: &str = "00-1bf7651916cd43dd8448eb211c80319d-c7ad6b7169203332-01";

fn context(trace_parent: &str) -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(trace_parent.to_string()),
        ..AgentTelemetryContext::default()
    }
}

/// A fixture whose one tool is checkpoint-required, so the run parks on an
/// approval before anything dispatches.
fn checkpointed_fixture() -> Fixture {
    fixture_with_tool_spec(AgentEffectSpec::non_idempotent().with_checkpoint_required())
}

/// A fixture whose one tool is non-idempotent and ungated, so an attempt
/// reported ambiguous parks the run on a reconciliation checkpoint.
fn reconciliation_fixture() -> Fixture {
    fixture_with_tool_spec(AgentEffectSpec::non_idempotent())
}

/// A tool-calling turn and a proposing turn, over one tool of `spec`.
fn fixture_with_tool_spec(spec: AgentEffectSpec) -> Fixture {
    use std::sync::atomic::AtomicU64;

    let tool = AgentToolId::new("charge-card").expect("tool id");
    let policies = AgentEffectPolicies::new()
        .with_tool_spec(tool.clone(), spec)
        .expect("the tool spec is valid");
    Fixture::with_sink(
        ScriptedDispatcher::new()
            .with_turn(
                AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                    .with_text("Charging the card.")
                    .with_tool_call(
                        AgentToolCallRequest::new(
                            AgentToolCallId::new("call-1").expect("call id"),
                            tool,
                            serde_json::json!({ "amount": 42 }),
                        )
                        .expect("the tool call is bounded"),
                    ),
            )
            .with_turn(
                AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                    .with_text("Done.")
                    .with_proposal(
                        AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                            .expect("the proposal is inline-bounded"),
                    ),
            ),
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
}

/// Parks a traced run on its approval checkpoint and answers the checkpoint
/// and the effect it gates.
async fn park(fx: &Fixture) -> (rakka_agent::AgentCheckpoint, AgentRunEffect) {
    fx.instantiate_agent().await;
    fx.create_task_traced(context(INGRESS_PARENT)).await;
    fx.pump().await.expect("the run parks on its checkpoint");
    parked(fx).await
}

/// The one open checkpoint of a parked run, and the effect it gates, read
/// from the durable record.
async fn parked(fx: &Fixture) -> (rakka_agent::AgentCheckpoint, AgentRunEffect) {
    let state = load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    let loop_state = state.loop_state().expect("the loop exists");
    let checkpoint = loop_state
        .open_checkpoints()
        .first()
        .expect("the approval checkpoint is open")
        .clone();
    let effect = loop_state
        .effects()
        .iter()
        .find(|effect| effect.effect_id == checkpoint.bound_effect.effect_id)
        .expect("the gated effect is on the loop")
        .clone();
    (checkpoint, effect)
}

fn approve(checkpoint: &rakka_agent::AgentCheckpoint, key: &str) -> AgentRunEntityCommand {
    decide(
        checkpoint,
        key,
        AgentApprovalDecision::Approve {
            credential_binding: None,
            expires_at: AgentTimestampMillis::new(1_000_000),
            allowed_use_count: 1,
        },
    )
}

fn deny(checkpoint: &rakka_agent::AgentCheckpoint, key: &str) -> AgentRunEntityCommand {
    decide(
        checkpoint,
        key,
        AgentApprovalDecision::Deny {
            reason: "not-authorized".to_string(),
        },
    )
}

fn decide(
    checkpoint: &rakka_agent::AgentCheckpoint,
    key: &str,
    decision: AgentApprovalDecision,
) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            key,
        )
        .expect("the decision key derives"),
        checkpoint_id: checkpoint.checkpoint_id.clone(),
        resolver: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "approver".to_string(),
            display_name: None,
        },
        decision: Box::new(AgentCheckpointDecision::Approval(decision)),
        telemetry: context(REQUEST_PARENT),
    }
}

fn closed(sink: &InMemoryAgentSegmentSink, wanted: &str) -> Vec<AgentTelemetrySegment> {
    sink.segments()
        .into_iter()
        .filter(|segment| match wanted {
            "resolve" => matches!(segment.operation, AgentSegmentOperation::CheckpointResolve),
            "resume" => matches!(segment.operation, AgentSegmentOperation::RunResume),
            other => panic!("unknown segment class {other}"),
        })
        .collect()
}

/// The resolution of one clean call: how many resolve and resume segments a
/// resolution closes when nothing goes wrong. The measure every other arm is
/// held to.
async fn clean_resolution() -> (usize, usize) {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = checkpointed_fixture().with_segments(sink.clone());
    let (checkpoint, _) = park(&fx).await;
    let resumes_before = closed(&sink, "resume").len();
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let reply = run
        .apply(approve(&checkpoint, "d1"), &fx.router, fx.now())
        .await
        .expect("the decision applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    (
        closed(&sink, "resolve").len(),
        closed(&sink, "resume").len() - resumes_before,
    )
}

#[tokio::test]
async fn a_conflict_point_loses_the_armed_write_with_a_revision_conflict() {
    use rakka_persistence::{DurableError, DurableStateStore};

    let fx = checkpointed_fixture();
    park(&fx).await;
    let id = run_scope().persistence_id();
    let before = fx
        .runs
        .load(&id)
        .await
        .expect("loads")
        .expect("the run exists");
    fx.runs.crash_at(1, CrashPoint::ConflictBeforeWrite);
    let lost = fx
        .runs
        .compare_and_set(&id, before.revision, before.state)
        .await
        .expect_err("a second writer moved the record first");
    fx.runs
        .assert_crash_fired(1, CrashPoint::ConflictBeforeWrite);
    fx.runs.survive();
    assert!(
        matches!(lost, DurableError::RevisionConflict { .. }),
        "{lost:?}"
    );
    let after = fx
        .runs
        .load(&id)
        .await
        .expect("loads")
        .expect("the run exists");
    assert_ne!(
        after.revision, before.revision,
        "the second writer's write is what moved the record"
    );
}

#[tokio::test]
async fn a_resolution_whose_settle_pass_loses_a_write_still_leaves_its_trace_once() {
    let (clean_resolves, clean_resumes) = clean_resolution().await;
    assert_eq!(clean_resolves, 1, "the measure itself");

    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = checkpointed_fixture().with_segments(sink.clone());
    let (checkpoint, effect) = park(&fx).await;
    let resumes_before = closed(&sink, "resume").len();

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    // Write 1 is the resolving transition. Write 2 is the settle pass's
    // first compare-and-set, which a second writer beats.
    fx.runs.crash_at(2, CrashPoint::ConflictBeforeWrite);
    let first = run
        .apply(approve(&checkpoint, "d1"), &fx.router, fx.now())
        .await;
    fx.runs
        .assert_crash_fired(2, CrashPoint::ConflictBeforeWrite);
    fx.runs.survive();
    assert!(
        first.is_err(),
        "the settle pass lost its write, and the call says so: {first:?}"
    );

    // The resolution is durable, so its trace exists: once, under the
    // identity the park linked forward to, a child of the run's ingress, and
    // linking the parked span and the request.
    let resolved = closed(&sink, "resolve");
    assert_eq!(resolved.len(), 1, "{:?}", sink.operations());
    let identity =
        AgentCheckpoint::resolve_span_identity(&effect.telemetry, &checkpoint.checkpoint_id)
            .expect("the resolve identity derives");
    assert_eq!(
        resolved[0].span_id.as_deref(),
        Some(identity.span_id.as_str())
    );
    assert_eq!(
        resolved[0].telemetry.trace_parent.as_deref(),
        Some(INGRESS_PARENT),
        "the run's context was read before the write that dropped the cached record"
    );
    for kind in [LINK_KIND_PARKED_CHECKPOINT, LINK_KIND_RESUME_REQUEST] {
        assert!(
            resolved[0].telemetry.span_links.iter().any(|link| {
                link.attributes.get(ATTR_AGENT_TELEMETRY_LINK_KIND) == Some(&kind.to_string())
            }),
            "a `{kind}` link: {:?}",
            resolved[0].telemetry.span_links
        );
    }

    // The re-drive finds the operation applied, resolves nothing, and closes
    // no second resolve segment.
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let replay = run
        .apply(approve(&checkpoint, "d1"), &fx.router, fx.now())
        .await
        .expect("the replay answers");
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "{replay:?}"
    );
    assert_eq!(
        closed(&sink, "resolve").len(),
        clean_resolves,
        "a duplicate resolved nothing and closes nothing"
    );
    assert_eq!(
        closed(&sink, "resume").len() - resumes_before,
        clean_resumes,
        "across the errored call and its re-drive, the wait's end is recorded as often as a \
         clean call records it"
    );
}

/// Parks a traced run on a reconciliation checkpoint: the model call is
/// answered, and the tool attempt it asked for is reported ambiguous.
async fn park_indeterminate(fx: &Fixture) -> (AgentCheckpoint, AgentRunEffect) {
    fx.instantiate_agent().await;
    fx.create_task_traced(context(INGRESS_PARENT)).await;
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    fx.dispatcher
        .drive(&mut run, &fx.router, fx.now())
        .await
        .expect("the model call is answered");
    let tool = run
        .state()
        .expect("state reads")
        .loop_state()
        .expect("the loop exists")
        .effects()
        .iter()
        .find(|effect| effect.request.tool_call().is_some())
        .expect("the tool effect exists")
        .clone();
    run.apply(
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: tool
                .result_operation_id(&run_scope())
                .expect("the operation id derives"),
            effect_id: tool.effect_id.clone(),
            generation: tool.generation,
            attempt: 1,
            fence: 1,
            outcome: Box::new(AgentRunEffectOutcome::Indeterminate {
                code: "connection-lost".to_string(),
                message: "the worker died mid-invocation".to_string(),
            }),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the ambiguity records");
    parked(fx).await
}

/// The operator's word that the ambiguous attempt completed: the decision
/// whose own transition ends the tool wait, so the settle pass after it
/// records the turn.
fn confirm_completed(checkpoint: &AgentCheckpoint, key: &str) -> AgentRunEntityCommand {
    AgentRunEntityCommand::ResolveCheckpoint {
        operation_id: AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            key,
        )
        .expect("the decision key derives"),
        checkpoint_id: checkpoint.checkpoint_id.clone(),
        resolver: PrincipalRef {
            principal_type: "user".to_string(),
            principal_id: "operator".to_string(),
            display_name: None,
        },
        decision: Box::new(AgentCheckpointDecision::Reconciliation(
            AgentReconciliationDecision::ConfirmedCompleted {
                resolution: Box::new(AgentEffectResolution::ConfirmedExecuted {
                    outcome: Box::new(AgentRunEffectOutcome::Tool {
                        call_id: AgentToolCallId::new("call-1").expect("call id"),
                        content: AgentTaskContent::inline(serde_json::json!({ "receipt": "r-9" }))
                            .expect("the content is inline-bounded"),
                    }),
                }),
            },
        )),
        telemetry: context(REQUEST_PARENT),
    }
}

/// A session store behind an outage switch: appends fail while `down` is
/// thrown, and reads and purges pass through. A settle pass that owes the
/// store an entry surfaces the outage as a memory error, which is a failure
/// `apply` re-answers against a re-read record.
#[derive(Clone, Default)]
struct OutageSessions {
    inner: InMemorySessionMemoryStore,
    down: Arc<AtomicBool>,
    refused: Arc<AtomicUsize>,
}

impl SessionMemoryStore for OutageSessions {
    fn backend_name(&self) -> &'static str {
        "outage-sessions"
    }

    fn append<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        entry: &'a SessionMemoryEntry,
    ) -> MemoryFuture<'a, SessionMemoryEntry> {
        if self.down.load(Ordering::SeqCst) {
            self.refused.fetch_add(1, Ordering::SeqCst);
            return Box::pin(async {
                Err(MemoryError::Backend {
                    backend: "outage-sessions".to_string(),
                    message: "the session store is unreachable".to_string(),
                })
            });
        }
        self.inner.append(scope, entry)
    }

    fn read<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        cursor: SessionMemoryCursor,
    ) -> MemoryFuture<'a, SessionMemoryPage> {
        self.inner.read(scope, cursor)
    }

    fn purge_run<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        policy: &'a SessionRetentionPolicy,
        terminal_at: AgentTimestampMillis,
        now: AgentTimestampMillis,
    ) -> MemoryFuture<'a, SessionPurgeOutcome> {
        self.inner.purge_run(scope, policy, terminal_at, now)
    }
}

/// The fixture's run store behind a one-shot load outage: once `armed`, the
/// next load fails as a store outage would, and every other call passes
/// through to the same durable record.
#[derive(Clone)]
struct RereadOutage {
    inner: RunStore,
    armed: Arc<AtomicBool>,
    failed: Arc<AtomicUsize>,
}

impl DurableStateStore<AgentRunState> for RereadOutage {
    fn backend_name(&self) -> &'static str {
        "reread-outage"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<AgentRunState>>> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.failed.fetch_add(1, Ordering::SeqCst);
            return Box::pin(async {
                Err(DurableError::store(
                    "reread-outage",
                    "the run store is unreachable",
                ))
            });
        }
        self.inner.load(persistence_id)
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: AgentRunState,
    ) -> StoreFuture<'a, StateRecord<AgentRunState>> {
        self.inner
            .compare_and_set(persistence_id, expected_revision, state)
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        self.inner.delete(persistence_id, expected_revision)
    }
}

/// A resolution whose settle pass fails in a way the call re-answers, and
/// whose re-read of the record then fails too, still leaves its trace on
/// that call: the transition committed before either fault, and the
/// segment's subject is the transition. The call answers the re-read's
/// error, and the re-drive, a duplicate, closes nothing more.
///
/// A lost compare-and-set is not the first fault here, because `apply`
/// answers a persistence failure as it stands and never re-reads for one.
/// A memory outage is re-answered, so the settle pass meets one: the
/// confirmed attempt ends the tool wait in the resolving transition, and the
/// turn it records owes the session store its entries.
#[tokio::test]
async fn a_resolution_whose_re_read_fails_after_its_settle_pass_still_leaves_its_trace_once() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let sessions = OutageSessions::default();
    let fx = reconciliation_fixture()
        .with_segments(sink.clone())
        .with_memory(AgentRunMemory::new(
            Arc::new(sessions.clone()),
            Arc::new(InMemoryContextSnapshotStore::new()),
        ));
    let (checkpoint, effect) = park_indeterminate(&fx).await;

    // The resolving entity reads its record through a store whose next load
    // fails, and the session store is down for the settle pass.
    let store = RereadOutage {
        inner: fx.runs.clone(),
        armed: Arc::new(AtomicBool::new(false)),
        failed: Arc::new(AtomicUsize::new(0)),
    };
    let mut run = AgentRunEntityStore::new(run_scope(), store.clone(), fx.effects.clone())
        .with_effect_policies(fx.policies.clone())
        .with_memory(fx.memory.clone().expect("the fixture wires memory"))
        .with_segments(sink.clone());
    run.recover(fx.now()).await.expect("the run recovers");
    let resumes_before = closed(&sink, "resume").len();
    sessions.down.store(true, Ordering::SeqCst);
    store.armed.store(true, Ordering::SeqCst);
    let first = run
        .apply(confirm_completed(&checkpoint, "d1"), &fx.router, fx.now())
        .await;
    assert!(
        sessions.refused.load(Ordering::SeqCst) > 0,
        "the settle pass after the resolution met the session outage"
    );
    assert_eq!(
        store.failed.load(Ordering::SeqCst),
        1,
        "the settle pass's error was re-answered, and the re-read failed"
    );
    assert!(
        matches!(first, Err(AgentRunError::Choreography(_))),
        "the call answers the re-read's error: {first:?}"
    );
    let state = load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    assert!(
        state
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()
            .is_empty(),
        "the resolution committed before either fault"
    );

    // The resolution is durable, so its trace exists: once, under the
    // identity the park linked forward to.
    let resolved = closed(&sink, "resolve");
    assert_eq!(resolved.len(), 1, "{:?}", sink.operations());
    let identity =
        AgentCheckpoint::resolve_span_identity(&effect.telemetry, &checkpoint.checkpoint_id)
            .expect("the resolve identity derives");
    assert_eq!(
        resolved[0].span_id.as_deref(),
        Some(identity.span_id.as_str())
    );

    // With both stores back, the re-drive finds the operation applied,
    // resolves nothing, and closes no second resolve segment.
    sessions.down.store(false, Ordering::SeqCst);
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let replay = run
        .apply(confirm_completed(&checkpoint, "d1"), &fx.router, fx.now())
        .await
        .expect("the replay answers");
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "{replay:?}"
    );
    assert_eq!(
        closed(&sink, "resolve").len(),
        1,
        "a duplicate resolved nothing and closes nothing: {:?}",
        sink.operations()
    );
    // A clean confirmation's settle pass parks the run on its next model
    // call before the phase is read, so it closes no resume segment, and
    // neither call here closes one either.
    assert_eq!(
        closed(&sink, "resume").len(),
        resumes_before,
        "{:?}",
        sink.operations()
    );
}

#[tokio::test]
async fn a_replayed_resolution_closes_nothing() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = checkpointed_fixture().with_segments(sink.clone());
    let (checkpoint, _) = park(&fx).await;
    for expected in ["applied", "duplicate"] {
        let mut run = fx.run();
        run.recover(fx.now()).await.expect("the run recovers");
        let reply = run
            .apply(approve(&checkpoint, "d1"), &fx.router, fx.now())
            .await
            .expect("the decision answers");
        match expected {
            "applied" => assert!(matches!(reply, AgentRunEntityReply::Applied { .. })),
            _ => assert!(matches!(reply, AgentRunEntityReply::Duplicate { .. })),
        }
        assert_eq!(
            closed(&sink, "resolve").len(),
            1,
            "after the {expected} call"
        );
    }
}

#[tokio::test]
async fn a_refused_resolution_closes_nothing() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = checkpointed_fixture().with_segments(sink.clone());
    let (checkpoint, _) = park(&fx).await;
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    // A lost first write: the transition itself never committed.
    fx.runs.crash_at(1, CrashPoint::ConflictBeforeWrite);
    let refused = run
        .apply(approve(&checkpoint, "d1"), &fx.router, fx.now())
        .await;
    fx.runs
        .assert_crash_fired(1, CrashPoint::ConflictBeforeWrite);
    fx.runs.survive();
    assert!(refused.is_err(), "{refused:?}");
    assert!(
        closed(&sink, "resolve").is_empty(),
        "nothing committed, so nothing was resolved: {:?}",
        sink.operations()
    );
}

/// How many resolve and resume segments the sink holds, since `before`.
fn counts_since(sink: &InMemoryAgentSegmentSink, before: (usize, usize)) -> (usize, usize) {
    (
        closed(sink, "resolve").len() - before.0,
        closed(sink, "resume").len() - before.1,
    )
}

/// A denial fails the gated effect and ends the run in its own transition, so
/// the commit alone decides what a clean call closes. A denial whose settle
/// pass lost its first write closes all of it on that first call, and its
/// re-drive closes nothing more.
#[tokio::test]
async fn a_denial_that_ends_the_run_closes_on_its_first_call_what_a_clean_one_closes() {
    // The clean denial is the measure.
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = checkpointed_fixture().with_segments(sink.clone());
    let (checkpoint, _) = park(&fx).await;
    let before = counts_since(&sink, (0, 0));
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    fx.runs.reset_writes();
    let reply = run
        .apply(deny(&checkpoint, "d1"), &fx.router, fx.now())
        .await
        .expect("the denial applies");
    assert!(matches!(reply, AgentRunEntityReply::Applied { .. }));
    assert!(
        fx.runs.writes() >= 2,
        "the settle pass writes after the transition"
    );
    let state = load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    assert!(
        state.status().is_some_and(AgentRunStatus::is_terminal),
        "a denial ends the run: {:?}",
        state.status()
    );
    let clean = counts_since(&sink, before);
    assert_eq!(
        clean,
        (1, 1),
        "a clean denial resolves the checkpoint once and ends the wait once: {:?}",
        sink.operations()
    );

    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = checkpointed_fixture().with_segments(sink.clone());
    let (checkpoint, _) = park(&fx).await;
    let before = counts_since(&sink, (0, 0));
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    // Write 1 is the denying transition. Write 2 is the settle pass's first
    // compare-and-set, which a second writer beats.
    fx.runs.crash_at(2, CrashPoint::ConflictBeforeWrite);
    let first = run
        .apply(deny(&checkpoint, "d1"), &fx.router, fx.now())
        .await;
    fx.runs
        .assert_crash_fired(2, CrashPoint::ConflictBeforeWrite);
    fx.runs.survive();
    assert!(
        first.is_err(),
        "the settle pass lost its write, and the call says so: {first:?}"
    );
    assert_eq!(
        counts_since(&sink, before),
        clean,
        "the first call closes everything a clean denial closes: {:?}",
        sink.operations()
    );

    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    let replay = run
        .apply(deny(&checkpoint, "d1"), &fx.router, fx.now())
        .await
        .expect("the replay answers");
    assert!(
        matches!(replay, AgentRunEntityReply::Duplicate { .. }),
        "{replay:?}"
    );
    assert_eq!(
        counts_since(&sink, before),
        clean,
        "the re-drive closes nothing more: {:?}",
        sink.operations()
    );
}

/// An ordinary run — a model call, a tool call, and a model call that
/// proposes — closes the one `run-resume` segment it closed before a
/// resolution's segment was decided at its commit: wherever the record
/// survives the call, the resume rule is the one it always was. The same
/// run as `telemetry_segments.rs`'s full run.
#[tokio::test]
async fn an_ordinary_run_closes_the_resume_segments_it_always_closed() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = Fixture::new(
        ScriptedDispatcher::with_adapter(
            DeterministicModelAdapter::new()
                .with_turn_for(
                    1,
                    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                        .with_text("Let me look that up.")
                        .with_tool_call(
                            AgentToolCallRequest::new(
                                AgentToolCallId::new("call-1").expect("call id"),
                                AgentToolId::new("lookup").expect("tool id"),
                                serde_json::json!({ "query": "ticket" }),
                            )
                            .expect("the tool call is bounded"),
                        ),
                )
                .with_turn_for(
                    2,
                    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                        .with_text("I have an answer.")
                        .with_proposal(
                            AgentTaskContent::inline(serde_json::json!({ "answer": "resolved" }))
                                .expect("the proposal is inline-bounded"),
                        ),
                ),
        )
        .with_tool_result(
            "lookup",
            AgentTaskContent::inline(serde_json::json!({ "found": true }))
                .expect("the tool result is inline-bounded"),
        ),
    )
    .with_segments(sink.clone());
    fx.instantiate_agent().await;
    fx.create_task_traced(context(INGRESS_PARENT)).await;
    fx.pump().await.expect("the run completes");
    let state = load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    assert_eq!(state.status(), Some(AgentRunStatus::Completed));
    // Three waits are discharged here (the first model call, the tool call,
    // the second model call), and the first two calls that discharge one
    // park the run on the next wait in their own settle pass. The count is
    // the one the run closed before this rule existed.
    assert_eq!(closed(&sink, "resume").len(), 1, "{:?}", sink.operations());
}
