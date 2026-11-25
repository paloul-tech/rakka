//! Issue #25: a run's effect flush loads the workflow record once per round,
//! whatever the number of `Ready` effects it hands to the outbox.
//!
//! The first half drives [`AgentRunEffectSink::dispatch_all`] on the durable
//! workflow sink directly: one load for a batch, a lost compare-and-set
//! reloaded and retried (at most three more times), any other failure ending
//! the batch at the effect it hit, and the provided default going effect by
//! effect for a sink that only implements `dispatch`.

use std::sync::{Arc, Mutex};

use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter};
use rakka_agent::{
    AgentEffectError, AgentEffectFuture, AgentEffectSpec, AgentModelTurn, AgentRunEffect,
    AgentRunEffectSink, AgentRunScope, AgentTaskContent, AgentToolAuthority, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, WorkflowAgentRunEffectSink,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::substrate::{OutboxCommand, OutboxStatus, OutboxTarget};
use rakka_agent_workflow::AgentEffect;
use rakka_persistence::DurableStateStore;

mod common;

use common::*;

const TOOL: &str = "charge-card";

/// The bound on re-applying one effect's schedule after a lost
/// compare-and-set (the dispatcher's own bound for its writes, issue #81).
const REAPPLICATIONS: usize = 3;

fn tool_call_request(call: &str) -> AgentToolCallRequest {
    AgentToolCallRequest::new(
        AgentToolCallId::new(call).expect("the call id is valid"),
        AgentToolId::new(TOOL).expect("the tool id is valid"),
        serde_json::json!({ "amount": 42 }),
    )
    .expect("the tool call is bounded")
}

/// Turn one calls the tool three times; turn two proposes.
fn three_calls_then_proposal() -> DeterministicModelAdapter {
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
        )
}

/// A run whose model turn proposed three tool calls, all three `Ready` and
/// unresolved: nothing has claimed them.
async fn three_ready_tool_effects() -> (AuthorityFixture, Vec<AgentRunEffect>) {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let fx = AuthorityFixture::new(
        three_calls_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let mut effects = Vec::new();
    for slot in 1..=3 {
        let effect = fx
            .effect_at(slot)
            .await
            .expect("the tool effect is committed");
        assert_eq!(
            effect.status,
            rakka_agent::AgentRunEffectStatus::Ready,
            "slot {slot} is ready"
        );
        effects.push(effect);
    }
    (fx, effects)
}

fn workflow_effects(effects: &[AgentRunEffect]) -> Vec<AgentEffect> {
    effects
        .iter()
        .map(|effect| effect.to_workflow_effect(&run_scope()))
        .collect()
}

/// Deletes the run's workflow record, so the next schedule of each row is a
/// write. It loads the record once itself.
async fn delete_workflow_record(fx: &AuthorityFixture) {
    let id = workflow_persistence_id(&run_scope());
    let record = fx
        .workflow_store
        .load(&id)
        .await
        .expect("the workflow store loads")
        .expect("the run has a workflow record");
    fx.workflow_store
        .delete(&id, record.revision)
        .await
        .expect("the workflow record is deleted");
}

/// Schedules `count` unrelated outbox rows on the run's workflow record,
/// creating the record if it is absent.
async fn seed_rows(fx: &AuthorityFixture, count: usize) {
    let mut inbox = rakka_agent_workflow::AgentRunInbox::with_clock(
        rakka_agent::workflow_run_id(&run_scope()),
        fx.workflow_store.clone(),
        fx.wf_clock.clone(),
    );
    inbox.recover().await.expect("the inbox recovers");
    for n in 0..count {
        inbox
            .inner_mut()
            .schedule_outbox(OutboxCommand::new(
                format!("seed-{n}"),
                OutboxTarget::application("seed"),
                "rakka.test.Seed",
                b"seed".to_vec(),
            ))
            .await
            .expect("the seed row is scheduled");
    }
}

fn workflow_sink(
    fx: &AuthorityFixture,
) -> WorkflowAgentRunEffectSink<WorkflowStore, rakka_agent::testkit::SharedAtomicWorkflowClock> {
    WorkflowAgentRunEffectSink::new(fx.workflow_store.clone(), fx.wf_clock.clone())
}

/// Hands `effects` to `sink`, returning the call's result and the indices it
/// reported accepted, in order.
async fn dispatch_all<S: AgentRunEffectSink>(
    sink: &S,
    effects: &[AgentEffect],
) -> (Result<(), AgentEffectError>, Vec<usize>) {
    let mut accepted = Vec::new();
    let result = {
        let mut report = |index: usize| accepted.push(index);
        sink.dispatch_all(&run_scope(), effects, &mut report).await
    };
    (result, accepted)
}

async fn row_status(fx: &AuthorityFixture, effect: &AgentRunEffect) -> Option<OutboxStatus> {
    fx.outbox_row(effect).await.map(|row| row.status())
}

fn sink_code(error: &AgentEffectError) -> &str {
    match error {
        AgentEffectError::Sink { code, .. } => code,
        other => panic!("expected a sink error, got {other:?}"),
    }
}

#[tokio::test]
async fn dispatch_all_loads_the_record_once_for_every_effect() {
    let (fx, effects) = three_ready_tool_effects().await;
    delete_workflow_record(&fx).await;
    let records = workflow_effects(&effects);
    let sink = workflow_sink(&fx);

    fx.workflow_store.reset_loads();
    fx.workflow_store.reset_writes();
    let (result, accepted) = dispatch_all(&sink, &records).await;
    result.expect("the batch is accepted");
    assert_eq!(accepted, vec![0, 1, 2]);
    assert_eq!(fx.workflow_store.loads(), 1, "one load for the whole batch");
    assert_eq!(fx.workflow_store.writes(), 3, "one write per new row");

    // Again, with every row present: duplicates, still one load, no write.
    fx.workflow_store.reset_loads();
    fx.workflow_store.reset_writes();
    let (result, accepted) = dispatch_all(&sink, &records).await;
    result.expect("the duplicates are accepted");
    assert_eq!(accepted, vec![0, 1, 2]);
    assert_eq!(fx.workflow_store.loads(), 1);
    assert_eq!(fx.workflow_store.writes(), 0, "a duplicate writes nothing");

    for effect in &effects {
        assert_eq!(row_status(&fx, effect).await, Some(OutboxStatus::Pending));
    }
}

#[tokio::test]
async fn a_conflict_mid_batch_is_retried_and_every_effect_is_accepted() {
    let (fx, effects) = three_ready_tool_effects().await;
    delete_workflow_record(&fx).await;
    let records = workflow_effects(&effects);
    let sink = workflow_sink(&fx);

    fx.workflow_store.reset_loads();
    fx.workflow_store
        .crash_at(2, CrashPoint::ConflictBeforeWrite);
    let (result, accepted) = dispatch_all(&sink, &records).await;
    fx.workflow_store
        .assert_crash_fired(2, CrashPoint::ConflictBeforeWrite);
    fx.workflow_store.survive();

    result.expect("a lost compare-and-set is reloaded and retried");
    assert_eq!(accepted, vec![0, 1, 2]);
    assert_eq!(
        fx.workflow_store.loads(),
        2,
        "the batch's load, and one reload for the conflict"
    );
    for effect in &effects {
        assert_eq!(row_status(&fx, effect).await, Some(OutboxStatus::Pending));
    }
}

#[tokio::test]
async fn a_conflict_on_the_first_write_is_retried_and_reports_each_acceptance_once() {
    let (fx, effects) = three_ready_tool_effects().await;
    delete_workflow_record(&fx).await;
    let records = workflow_effects(&effects);
    let sink = workflow_sink(&fx);
    // A conflict needs a record for the other writer to move: an absent
    // record's first write creates it. So an unrelated row goes in first, and
    // the batch's first write lands on an existing record.
    seed_rows(&fx, 1).await;

    fx.workflow_store.reset_loads();
    fx.workflow_store
        .crash_at(1, CrashPoint::ConflictBeforeWrite);
    let (result, accepted) = dispatch_all(&sink, &records).await;
    fx.workflow_store
        .assert_crash_fired(1, CrashPoint::ConflictBeforeWrite);
    fx.workflow_store.survive();

    result.expect("a lost first compare-and-set is reloaded and retried");
    assert_eq!(accepted, vec![0, 1, 2], "each acceptance is reported once");
    assert_eq!(fx.workflow_store.loads(), 2);
    for effect in &effects {
        assert_eq!(row_status(&fx, effect).await, Some(OutboxStatus::Pending));
    }
}

#[tokio::test]
async fn a_conflict_past_the_bound_ends_the_batch() {
    let (fx, effects) = three_ready_tool_effects().await;
    delete_workflow_record(&fx).await;
    let records = workflow_effects(&effects);
    let sink = workflow_sink(&fx);

    fx.workflow_store.reset_loads();
    fx.workflow_store.conflict_from(2);
    let (result, accepted) = dispatch_all(&sink, &records).await;
    fx.workflow_store.survive();

    let error = result.expect_err("a conflict that keeps losing ends the batch");
    assert_eq!(sink_code(&error), "outbox-schedule-failed");
    assert_eq!(accepted, vec![0]);
    assert_eq!(
        fx.workflow_store.loads(),
        1 + REAPPLICATIONS,
        "the batch's load, and one reload per re-application"
    );
    assert_eq!(
        row_status(&fx, &effects[0]).await,
        Some(OutboxStatus::Pending)
    );
    assert_eq!(row_status(&fx, &effects[1]).await, None);
    assert_eq!(row_status(&fx, &effects[2]).await, None);
}

#[tokio::test]
async fn a_store_failure_ends_the_batch_at_the_effect_it_hit() {
    let (fx, effects) = three_ready_tool_effects().await;
    delete_workflow_record(&fx).await;
    let records = workflow_effects(&effects);
    let sink = workflow_sink(&fx);

    fx.workflow_store.reset_loads();
    fx.workflow_store.crash_at(2, CrashPoint::BeforeWrite);
    let (result, accepted) = dispatch_all(&sink, &records).await;
    fx.workflow_store
        .assert_crash_fired(2, CrashPoint::BeforeWrite);
    fx.workflow_store.survive();

    let error = result.expect_err("a store failure ends the batch");
    assert_eq!(sink_code(&error), "outbox-schedule-failed");
    assert_eq!(accepted, vec![0]);
    assert_eq!(
        fx.workflow_store.loads(),
        1,
        "a failure that is not a conflict is not retried"
    );
    assert_eq!(
        row_status(&fx, &effects[0]).await,
        Some(OutboxStatus::Pending)
    );
    assert_eq!(row_status(&fx, &effects[1]).await, None);
    assert_eq!(row_status(&fx, &effects[2]).await, None);
}

/// A sink that implements only `dispatch`, forwarding to an inner sink, as
/// rakka.host's roster wrapper does: it gets the provided `dispatch_all`.
#[derive(Clone)]
struct ForwardingSink<Inner> {
    inner: Inner,
    calls: Arc<Mutex<usize>>,
}

impl<Inner: AgentRunEffectSink> AgentRunEffectSink for ForwardingSink<Inner> {
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }

    fn dispatch<'a>(
        &'a self,
        scope: &'a AgentRunScope,
        effect: &'a AgentEffect,
    ) -> AgentEffectFuture<'a, ()> {
        *self.calls.lock().expect("the counter is not poisoned") += 1;
        self.inner.dispatch(scope, effect)
    }
}

#[tokio::test]
async fn the_default_dispatch_all_goes_effect_by_effect() {
    let (fx, effects) = three_ready_tool_effects().await;
    delete_workflow_record(&fx).await;
    let records = workflow_effects(&effects);
    let sink = ForwardingSink {
        inner: workflow_sink(&fx),
        calls: Arc::new(Mutex::new(0)),
    };

    fx.workflow_store.reset_loads();
    let (result, accepted) = dispatch_all(&sink, &records).await;
    result.expect("the default hands every effect over");
    assert_eq!(accepted, vec![0, 1, 2]);
    assert_eq!(*sink.calls.lock().expect("not poisoned"), 3);
    assert_eq!(
        fx.workflow_store.loads(),
        3,
        "a wrapper that does not forward dispatch_all keeps one load per effect"
    );
    for effect in &effects {
        assert_eq!(row_status(&fx, effect).await, Some(OutboxStatus::Pending));
    }
}
