//! Shared support for the dispatcher fleet's tests: a store that can make a
//! compare-and-set lose and run another writer between two calls, and the
//! helpers that put one effect into a run's outbox and one worker over the
//! stores.

// Each integration-test binary compiles this module independently and uses a
// different subset of it; what one binary leaves unused is not dead code.
#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rakka_agent_workflow::{
    AgentCausationId, AgentCorrelationId, AgentDeduplicationKey, AgentDispatchClaim,
    AgentDispatchEntry, AgentDispatcherFleetSettings, AgentDispatcherFleetState,
    AgentDispatcherWorker, AgentDispatcherWorkerId, AgentDurabilityMetadata, AgentEffect,
    AgentEffectDispatchFuture, AgentEffectDispatcher, AgentEffectId, AgentEffectKind,
    AgentEffectMetadata, AgentEffectSchedule, AgentEffectTarget, AgentIdempotencyKey, AgentRunId,
    AgentRunInbox, AgentTimestampMillis,
};
use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::{
    DurableError, DurableState, DurableStateStore, InMemoryDurableStateStore, PersistenceId,
    Revision, StateRecord, StoreFuture,
};
use rakka_workflow::{
    ManualWorkflowClock, OutboxDispatchResult, OutboxMessageId, OutboxStatus, WorkflowState,
    WorkflowTimestamp,
};

/// A one-shot piece of work a store runs between two of its calls.
pub type Hook = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// An in-memory durable state store that a test can contend.
///
/// It behaves as the plain in-memory store until a test arms it. Armed, it
/// stands in for a second writer: it can move the revision before a
/// compare-and-set so the caller's write loses, run a hook before the next
/// write or the next load, or refuse one write for a reason that is not a
/// lost compare-and-set.
pub struct ContendedStore<S>
where
    S: DurableState,
{
    inner: InMemoryDurableStateStore<S>,
    contend: Arc<Mutex<usize>>,
    refuse_next_write: Arc<AtomicBool>,
    before_write: Arc<Mutex<Option<Hook>>>,
    before_load: Arc<Mutex<Option<Hook>>>,
    writes: Arc<AtomicUsize>,
}

impl<S> Clone for ContendedStore<S>
where
    S: DurableState,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            contend: self.contend.clone(),
            refuse_next_write: self.refuse_next_write.clone(),
            before_write: self.before_write.clone(),
            before_load: self.before_load.clone(),
            writes: self.writes.clone(),
        }
    }
}

impl<S> ContendedStore<S>
where
    S: DurableState,
{
    /// A store nothing contends yet.
    pub fn new() -> Self {
        Self {
            inner: InMemoryDurableStateStore::new(),
            contend: Arc::new(Mutex::new(0)),
            refuse_next_write: Arc::new(AtomicBool::new(false)),
            before_write: Arc::new(Mutex::new(None)),
            before_load: Arc::new(Mutex::new(None)),
            writes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Makes each of the next `times` compare-and-sets lose: before each, the
    /// store rewrites the state it holds, which moves the revision and
    /// changes nothing else.
    pub fn contend(&self, times: usize) {
        *self.contend.lock().expect("the counter is not poisoned") = times;
    }

    /// Refuses the next compare-and-set with a store error that is not a
    /// lost compare-and-set.
    pub fn refuse_next_write(&self) {
        self.refuse_next_write.store(true, Ordering::SeqCst);
    }

    /// Runs `hook` once, before the next compare-and-set is applied. The
    /// hook is taken before it runs, so writes it makes do not fire it again.
    pub fn before_next_write(&self, hook: Hook) {
        *self
            .before_write
            .lock()
            .expect("the hook slot is not poisoned") = Some(hook);
    }

    /// Runs `hook` once, before the next load answers. The hook is taken
    /// before it runs, so loads it makes do not fire it again.
    pub fn before_next_load(&self, hook: Hook) {
        *self
            .before_load
            .lock()
            .expect("the hook slot is not poisoned") = Some(hook);
    }

    /// How many compare-and-sets were asked of this store, won or lost.
    pub fn writes(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }

    /// The plain store underneath: reads and writes that fire nothing.
    pub fn inner(&self) -> &InMemoryDurableStateStore<S> {
        &self.inner
    }
}

impl<S> Default for ContendedStore<S>
where
    S: DurableState,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S> DurableStateStore<S> for ContendedStore<S>
where
    S: DurableState,
{
    fn backend_name(&self) -> &'static str {
        "contended-in-memory"
    }

    fn load<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
    ) -> StoreFuture<'a, Option<StateRecord<S>>> {
        Box::pin(async move {
            let hook = self
                .before_load
                .lock()
                .expect("the hook slot is not poisoned")
                .take();
            if let Some(hook) = hook {
                hook().await;
            }
            self.inner.load(persistence_id).await
        })
    }

    fn compare_and_set<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
        state: S,
    ) -> StoreFuture<'a, StateRecord<S>> {
        Box::pin(async move {
            self.writes.fetch_add(1, Ordering::SeqCst);
            let hook = self
                .before_write
                .lock()
                .expect("the hook slot is not poisoned")
                .take();
            if let Some(hook) = hook {
                hook().await;
            }
            if self.refuse_next_write.swap(false, Ordering::SeqCst) {
                return Err(DurableError::store(
                    self.backend_name(),
                    "the store refused this write",
                ));
            }
            let contended = {
                let mut left = self.contend.lock().expect("the counter is not poisoned");
                let contended = *left > 0;
                *left = left.saturating_sub(1);
                contended
            };
            if contended {
                if let Some(record) = self.inner.load(persistence_id).await? {
                    self.inner
                        .compare_and_set(persistence_id, record.revision, record.state)
                        .await?;
                }
            }
            self.inner
                .compare_and_set(persistence_id, expected_revision, state)
                .await
        })
    }

    fn delete<'a>(
        &'a self,
        persistence_id: &'a PersistenceId,
        expected_revision: Revision,
    ) -> StoreFuture<'a, Revision> {
        self.inner.delete(persistence_id, expected_revision)
    }
}

/// The workflow store every fleet test schedules effects into.
pub type WorkflowStore = InMemoryDurableStateStore<WorkflowState>;
/// The fleet store, contendable.
pub type FleetStore = ContendedStore<AgentDispatcherFleetState>;
/// A workflow-tier worker over the two.
pub type TestWorker = AgentDispatcherWorker<FleetStore, WorkflowStore, ManualWorkflowClock>;

/// The lease every fleet test claims under.
pub const LEASE_MS: u64 = 1_000;

/// A clock at the instant every helper effect is due.
pub fn clock() -> ManualWorkflowClock {
    ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100))
}

/// A worker over the shared stores, with a batch of eight and [`LEASE_MS`].
pub fn worker(
    worker_id: &str,
    fleet_store: &FleetStore,
    workflow_store: &WorkflowStore,
    clock: &ManualWorkflowClock,
    metrics: &Arc<InMemoryMetricsRecorder>,
) -> TestWorker {
    AgentDispatcherWorker::with_clock_and_metrics(
        AgentDispatcherWorkerId::new(worker_id),
        fleet_store.clone(),
        workflow_store.clone(),
        AgentDispatcherFleetSettings::new(8, LEASE_MS),
        clock.clone(),
        metrics.clone(),
    )
}

/// A worker over the shared stores that claims under its own lease.
pub fn worker_with_lease(
    worker_id: &str,
    lease_ms: u64,
    fleet_store: &FleetStore,
    workflow_store: &WorkflowStore,
    clock: &ManualWorkflowClock,
    metrics: &Arc<InMemoryMetricsRecorder>,
) -> TestWorker {
    AgentDispatcherWorker::with_clock_and_metrics(
        AgentDispatcherWorkerId::new(worker_id),
        fleet_store.clone(),
        workflow_store.clone(),
        AgentDispatcherFleetSettings::new(8, lease_ms),
        clock.clone(),
        metrics.clone(),
    )
}

/// The stores, the clock and the recorder one fleet test shares between its
/// workers.
pub struct World {
    /// The fleet's store.
    pub fleet_store: FleetStore,
    /// The store every run's outbox lives in.
    pub workflow_store: WorkflowStore,
    /// The one clock.
    pub clock: ManualWorkflowClock,
    /// The recorder every worker records into.
    pub metrics: Arc<InMemoryMetricsRecorder>,
}

impl World {
    /// Empty stores and a clock at 100.
    pub fn new() -> Self {
        Self {
            fleet_store: FleetStore::new(),
            workflow_store: WorkflowStore::new(),
            clock: clock(),
            metrics: Arc::new(InMemoryMetricsRecorder::new()),
        }
    }

    /// A worker over this world's stores, under [`LEASE_MS`].
    pub fn worker(&self, worker_id: &str) -> TestWorker {
        worker(
            worker_id,
            &self.fleet_store,
            &self.workflow_store,
            &self.clock,
            &self.metrics,
        )
    }

    /// A worker over this world's stores, under its own lease.
    pub fn worker_with_lease(&self, worker_id: &str, lease_ms: u64) -> TestWorker {
        worker_with_lease(
            worker_id,
            lease_ms,
            &self.fleet_store,
            &self.workflow_store,
            &self.clock,
            &self.metrics,
        )
    }

    /// Schedules one effect into `run` and registers it through `worker`,
    /// without claiming it.
    pub async fn register_one(&self, worker: &mut TestWorker, run: &str, effect: &str) {
        let run_id = AgentRunId::new(run);
        schedule(&self.workflow_store, &self.clock, &run_id, effect).await;
        worker.recover().await.expect("the fleet recovers");
        worker
            .refresh_run(run_id, None)
            .await
            .expect("the run registers");
    }

    /// Schedules one effect into `run`, registers it through `worker`, and
    /// claims it. Nothing else may be claimable when this is called.
    pub async fn claim_one(
        &self,
        worker: &mut TestWorker,
        run: &str,
        effect: &str,
    ) -> AgentDispatchClaim {
        self.register_one(worker, run, effect).await;
        let mut batch = worker.claim_due().await.expect("the claim pass runs");
        assert_eq!(batch.claims.len(), 1, "exactly the new effect is claimed");
        batch.claims.remove(0)
    }

    /// Claims one new effect and dispatches it to success, so both its
    /// outbox row and its fleet entry are finished.
    pub async fn complete_one(&self, worker: &mut TestWorker, run: &str, effect: &str) {
        let claim = self.claim_one(worker, run, effect).await;
        let mut dispatcher = RecordingDispatcher::succeeding();
        worker
            .dispatch_claim(claim, &mut dispatcher)
            .await
            .expect("the effect dispatches");
    }

    /// The fleet's revision as the store holds it now.
    pub async fn fleet_revision(&self) -> Option<Revision> {
        fleet_record(&self.fleet_store)
            .await
            .map(|record| record.revision)
    }
}

impl Default for World {
    fn default() -> Self {
        Self::new()
    }
}

/// A tool-call effect due at the instant [`clock`] starts at.
pub fn tool_effect(effect_id: &str) -> AgentEffect {
    tool_effect_due(effect_id, 100)
}

/// A tool-call effect due at `due_at`.
pub fn tool_effect_due(effect_id: &str, due_at: u64) -> AgentEffect {
    let durability = AgentDurabilityMetadata::new(
        AgentDeduplicationKey::new(format!("dedupe-{effect_id}")),
        AgentCausationId::new(format!("cause-{effect_id}")),
        AgentCorrelationId::new(format!("correlation-{effect_id}")),
    );
    let metadata = AgentEffectMetadata::new(
        AgentEffectId::new(effect_id),
        durability,
        AgentIdempotencyKey::new(format!("idempotency-{effect_id}")),
        AgentTimestampMillis::new(100),
    )
    .expect("effect metadata should be valid")
    .due_at(AgentTimestampMillis::new(due_at));
    let target = AgentEffectTarget {
        target_type: "tool".to_string(),
        name: "research-tool".to_string(),
        address: Some("tool://research-tool".to_string()),
        attributes: BTreeMap::new(),
    };
    AgentEffectSchedule::new(AgentEffectKind::ToolCall, target, metadata)
        .expect("effect schedule should be valid")
        .expected_result_type("dispatch.result")
        .expect("expected result type should be valid")
        .into_effect()
        .expect("effect should validate")
}

/// Schedules one tool-call effect, due now, into a run's outbox.
pub async fn schedule(
    store: &WorkflowStore,
    clock: &ManualWorkflowClock,
    run_id: &AgentRunId,
    effect_id: &str,
) {
    schedule_effect(store, clock, run_id, tool_effect(effect_id)).await;
}

/// Schedules one effect into a run's outbox.
pub async fn schedule_effect(
    store: &WorkflowStore,
    clock: &ManualWorkflowClock,
    run_id: &AgentRunId,
    effect: AgentEffect,
) {
    let mut inbox = AgentRunInbox::with_clock(run_id.clone(), store.clone(), clock.clone());
    inbox.recover().await.expect("inbox should recover");
    inbox
        .schedule_effect(effect)
        .await
        .expect("effect should schedule");
}

/// The status of one effect's outbox row, or `None` when the row is absent.
pub async fn outbox_status(
    store: &WorkflowStore,
    clock: &ManualWorkflowClock,
    run_id: &AgentRunId,
    effect_id: &str,
) -> Option<OutboxStatus> {
    let mut inbox = AgentRunInbox::with_clock(run_id.clone(), store.clone(), clock.clone());
    inbox.recover().await.expect("inbox should recover");
    inbox
        .inner()
        .state()
        .expect("workflow state should be recovered")
        .outbox_entry(&OutboxMessageId::new(effect_id))
        .map(|entry| entry.status())
}

/// The fleet's durable record as the store holds it now, read without
/// firing any hook.
pub async fn fleet_record(store: &FleetStore) -> Option<StateRecord<AgentDispatcherFleetState>> {
    store
        .inner()
        .load(&rakka_agent_workflow::agent_dispatcher_fleet_persistence_id())
        .await
        .expect("the fleet store loads")
}

/// The fleet entry of one effect of one run, as the store holds it now.
pub async fn fleet_entry(
    store: &FleetStore,
    run_id: &AgentRunId,
    effect_id: &str,
) -> Option<AgentDispatchEntry> {
    fleet_record(store).await.and_then(|record| {
        record
            .state
            .entry(&rakka_agent_workflow::agent_dispatch_id(
                run_id,
                &AgentEffectId::new(effect_id),
            ))
            .cloned()
    })
}

/// A dispatcher that records what it was handed and answers from a script,
/// then with success.
pub struct RecordingDispatcher {
    results: VecDeque<OutboxDispatchResult>,
    /// The effects it was handed, in order.
    pub seen: Vec<AgentEffectId>,
}

impl RecordingDispatcher {
    /// A dispatcher that answers `results` in order, then success.
    pub fn new(results: impl IntoIterator<Item = OutboxDispatchResult>) -> Self {
        Self {
            results: results.into_iter().collect(),
            seen: Vec::new(),
        }
    }

    /// A dispatcher that always succeeds.
    pub fn succeeding() -> Self {
        Self::new([])
    }
}

impl AgentEffectDispatcher for RecordingDispatcher {
    fn dispatch<'a>(
        &'a mut self,
        job: &'a rakka_agent_workflow::AgentDispatchJob,
    ) -> AgentEffectDispatchFuture<'a> {
        self.seen.push(job.effect.effect_id.clone());
        let result = self
            .results
            .pop_front()
            .unwrap_or(OutboxDispatchResult::Success);
        Box::pin(async move { result })
    }
}
