//! Issue #25, measured on PostgreSQL: the workflow-record loads and wall time
//! of handing a run's three `Ready` effects to the outbox, effect by effect
//! (the flush before the change) against one `dispatch_all` batch (after it).
//!
//! Gated on `RAKKA_POSTGRES_TEST_DSN`; without it the test says it skipped
//! and passes. Only the workflow record is on PostgreSQL: the flush's
//! per-round reads are workflow-store loads, and the run store is not read
//! per round. The record grows with the run's history, so the measurement is
//! taken fresh and with 50 and 200 finished outbox rows. Loads are asserted;
//! wall time is printed, never asserted.
//!
//! ```sh
//! RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo test -p rakka-agent --all-features --release --test effect_flush_postgres -- --nocapture
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    AgentEffectSpec, AgentModelTurn, AgentRunEffect, AgentRunEffectSink, AgentRunId, AgentRunScope,
    AgentTaskContent, AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    WorkflowAgentRunEffectSink, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::{
    OutboxCommand, OutboxMessageId, OutboxTarget, SystemWorkflowClock, WorkflowState,
};
use rakka_agent_workflow::{AgentEffect, AgentRunInbox};
use rakka_persistence::{
    DurableError, DurableResult, DurableState, DurableStateStore, PersistenceId, Revision,
    StateCodec, StateRecord, StoreFuture,
};
use rakka_persistence_postgres::PostgresDurableStateStore;
use tokio_postgres::NoTls;

mod common;

use common::*;

const TOOL: &str = "charge-card";
const REPETITIONS: usize = 20;
/// Untimed rounds before each timed series, so connection and statement
/// warm-up is not charged to whichever series runs first.
const WARM_UP: usize = 5;
const HISTORY_SIZES: [usize; 3] = [0, 50, 200];

/// The workflow record, as JSON, for the PostgreSQL store.
#[derive(Clone)]
struct JsonWorkflowStateCodec;

impl StateCodec<WorkflowState> for JsonWorkflowStateCodec {
    fn encode(&self, state: &WorkflowState) -> DurableResult<Vec<u8>> {
        serde_json::to_vec(state).map_err(|error| DurableError::store("json", error.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> DurableResult<WorkflowState> {
        serde_json::from_slice(bytes)
            .map_err(|error| DurableError::store("json", error.to_string()))
    }
}

/// Counts every load a store is asked for, and forwards everything.
#[derive(Clone)]
struct CountingStore<Inner> {
    inner: Inner,
    loads: Arc<AtomicUsize>,
}

impl<Inner> CountingStore<Inner> {
    fn loads(&self) -> usize {
        self.loads.load(Ordering::SeqCst)
    }

    fn reset(&self) {
        self.loads.store(0, Ordering::SeqCst);
    }
}

impl<S, Inner> DurableStateStore<S> for CountingStore<Inner>
where
    S: DurableState,
    Inner: DurableStateStore<S>,
{
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.inner.load(persistence_id)
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>> {
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

fn tool_call_request(call: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new(call).expect("the call id is valid"),
        AgentToolId::new(TOOL).expect("the tool id is valid"),
        serde_json::json!({ "amount": 42 }),
    )
    .expect("the tool call is bounded")
}

/// Three `Ready` tool effects from a real (in-memory) run: turn one calls the
/// tool three times.
async fn three_ready_tool_effects() -> Vec<AgentRunEffect> {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let fx = AuthorityFixture::new(
        DeterministicModelAdapter::new()
            .with_turn_for(
                1,
                AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                    .with_tool_call(tool_call_request("call-1"))
                    .with_tool_call(tool_call_request("call-2"))
                    .with_tool_call(tool_call_request("call-3")),
            )
            .with_turn_for(
                2,
                AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_proposal(
                    AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                        .expect("the proposal is inline-bounded"),
                ),
            ),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let mut effects = Vec::new();
    for slot in 1..=3 {
        effects.push(
            fx.effect_at(slot)
                .await
                .expect("the tool effect is committed"),
        );
    }
    effects
}

/// A run scope no earlier run of this test used: the table is shared, so a
/// rerun must never find the previous run's record.
fn fresh_scope(history: usize) -> AgentRunScope {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_millis();
    let run = AgentRunId::new(format!("issue-25-measure-{history}-{millis}"))
        .expect("the run id is valid");
    AgentRunScope::new(tenant(), agent_id(), run).expect("the run scope is valid")
}

/// Seeds `count` finished outbox rows on the scope's workflow record.
async fn seed_history<Store>(store: &Store, scope: &AgentRunScope, count: usize)
where
    Store: DurableStateStore<WorkflowState>,
{
    let mut inbox = AgentRunInbox::with_clock(
        rakka_agent::workflow_run_id(scope),
        store.clone(),
        SystemWorkflowClock,
    );
    inbox.recover().await.expect("the inbox recovers");
    for n in 0..count {
        let id = format!("history-{n}");
        inbox
            .inner_mut()
            .schedule_outbox(OutboxCommand::new(
                id.clone(),
                OutboxTarget::application("history"),
                "rakka.test.History",
                b"history".to_vec(),
            ))
            .await
            .expect("the history row is scheduled");
        inbox
            .inner_mut()
            .record_outbox_success(&OutboxMessageId::new(id))
            .await
            .expect("the history row finishes");
    }
}

async fn outbox_rows<Store>(store: &Store, scope: &AgentRunScope) -> usize
where
    Store: DurableStateStore<WorkflowState>,
{
    let record = store
        .load(&workflow_persistence_id(scope))
        .await
        .expect("the workflow store loads")
        .expect("the record exists");
    serde_json::to_value(&record.state).expect("the state serializes")["outbox"]
        .as_object()
        .map_or(0, serde_json::Map::len)
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort();
    samples[samples.len() / 2]
}

#[tokio::test]
async fn per_round_workflow_loads_and_wall_time_on_postgres() {
    let Ok(dsn) = std::env::var("RAKKA_POSTGRES_TEST_DSN") else {
        println!("skipped: RAKKA_POSTGRES_TEST_DSN is not set");
        return;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .expect("PostgreSQL accepts the connection");
    tokio::spawn(connection);
    let postgres = PostgresDurableStateStore::new(client, JsonWorkflowStateCodec);
    postgres.migrate().await.expect("the schema migrates");
    let store = CountingStore {
        inner: postgres,
        loads: Arc::new(AtomicUsize::new(0)),
    };
    let sink = WorkflowAgentRunEffectSink::new(store.clone(), SystemWorkflowClock);
    let effects = three_ready_tool_effects().await;

    for history in HISTORY_SIZES {
        let scope = fresh_scope(history);
        seed_history(&store, &scope, history).await;
        let records: Vec<AgentEffect> = effects
            .iter()
            .map(|effect| effect.to_workflow_effect(&scope))
            .collect();
        // The first flush writes the three rows; every flush after it is the
        // steady state this measures, with the rows present.
        sink.dispatch_all(&scope, &records, &mut |_| {})
            .await
            .expect("the rows are scheduled");
        let rows = outbox_rows(&store, &scope).await;
        for _ in 0..WARM_UP {
            for record in &records {
                sink.dispatch(&scope, record)
                    .await
                    .expect("the duplicate is accepted");
            }
            sink.dispatch_all(&scope, &records, &mut |_| {})
                .await
                .expect("the duplicates are accepted");
        }

        let mut before = Vec::with_capacity(REPETITIONS);
        for _ in 0..REPETITIONS {
            store.reset();
            let started = Instant::now();
            for record in &records {
                sink.dispatch(&scope, record)
                    .await
                    .expect("the duplicate is accepted");
            }
            before.push(started.elapsed());
            assert_eq!(store.loads(), 3, "effect by effect: one load each");
        }

        let mut after = Vec::with_capacity(REPETITIONS);
        for _ in 0..REPETITIONS {
            store.reset();
            let started = Instant::now();
            sink.dispatch_all(&scope, &records, &mut |_| {})
                .await
                .expect("the duplicates are accepted");
            after.push(started.elapsed());
            assert_eq!(store.loads(), 1, "one batch: one load");
        }

        println!(
            "history={history} rows={rows} before: loads=3 median={:?} after: loads=1 median={:?}",
            median(before),
            median(after)
        );
    }
}
