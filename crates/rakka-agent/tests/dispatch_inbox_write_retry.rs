//! The dispatcher's own writes to a run's inbox survive a lost
//! compare-and-set (issue #81).
//!
//! A run's inbox store has a second writer in production: a deployment
//! sweep that applies `PromoteMemory` or `AppendClaim` into live runs
//! schedules an effect, and scheduling is a write to the same store the
//! dispatcher writes on every attempt. When the two meet, one of them loses
//! the store's compare-and-set. Each test here arms exactly that loss —
//! [`CrashPoint::ConflictBeforeWrite`] moves the record's revision between
//! the dispatcher's `recover()` and its write, a genuine `RevisionConflict`
//! from the store — at one of the dispatcher's own writes, and asserts the
//! pass re-reads and re-applies the write instead of aborting with the ticket
//! still claimed until its lease runs out.
//!
//! One pass of a claimed tool ticket writes the run's workflow store three
//! times: the dispatcher's `Dispatching` mark, then the result delivery's
//! scheduling of the next model call, then the dispatcher's success record.
//! The second is the delivery's write, not the dispatcher's, and is outside
//! this fix.

use std::sync::Arc;

use rakka_agent::testkit::{CrashPoint, DeterministicModelAdapter};
use rakka_agent::{
    AgentDispatchWindow, AgentEffectSpec, AgentModelTurn, AgentOperationId, AgentOperationKind,
    AgentRunEffect, AgentRunEffectStatus, AgentRunEntityCommand, AgentTaskContent,
    AgentToolAuthority, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION, METRIC_AGENT_DISPATCH_WRITE_RETRIES,
};
use rakka_agent_workflow::substrate::OutboxStatus;
use rakka_agent_workflow::{AgentDispatchEntry, AgentDispatchStatus};
use rakka_core::InMemoryMetricsRecorder;

mod common;

use common::*;

const TOOL: &str = "charge-card";

fn tool_then_proposal() -> DeterministicModelAdapter {
    DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_tool_call(
                AgentToolCallRequest::new(
                    AgentToolCallId::new("call-1").expect("the call id is valid"),
                    AgentToolId::new(TOOL).expect("the tool id is valid"),
                    serde_json::json!({ "amount": 42 }),
                )
                .expect("the tool call is bounded"),
            ),
        )
        .with_turn_for(
            2,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_proposal(
                AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                    .expect("the proposal is inline-bounded"),
            ),
        )
}

/// A run whose tool ticket is in the outbox, ready for the next pass to
/// claim, with the recorder its dispatch pipeline counts into.
async fn ready_tool_ticket() -> (AuthorityFixture, InMemoryMetricsRecorder, AgentRunEffect) {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let metrics = InMemoryMetricsRecorder::new();
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    )
    .with_metrics(Arc::new(metrics.clone()));
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    (fx, metrics, effect)
}

/// Every re-application the dispatcher counted, by the write that lost.
fn write_retries(metrics: &InMemoryMetricsRecorder) -> Vec<(String, f64)> {
    metrics
        .snapshot()
        .observations_named(METRIC_AGENT_DISPATCH_WRITE_RETRIES)
        .iter()
        .map(|observation| {
            (
                observation
                    .attribute("operation")
                    .unwrap_or_default()
                    .to_string(),
                observation.value(),
            )
        })
        .collect()
}

/// The dispatcher fleet's index entry for one effect's ticket.
async fn fleet_entry(fx: &AuthorityFixture, effect: &AgentRunEffect) -> AgentDispatchEntry {
    use rakka_persistence::DurableStateStore;
    let fleet_id = rakka_persistence::PersistenceId::new(
        rakka_agent_workflow::agent_dispatcher_fleet_persistence_id()
            .as_str()
            .to_string(),
    );
    let record = fx
        .fleet_store
        .load(&fleet_id)
        .await
        .expect("the fleet store loads")
        .expect("the fleet index exists");
    record
        .state
        .entry(&rakka_agent_workflow::agent_dispatch_id(
            &rakka_agent::workflow_run_id(&run_scope()),
            &effect.dispatch_ticket_id(),
        ))
        .cloned()
        .expect("the ticket is registered in the fleet index")
}

#[tokio::test]
async fn a_dispatching_mark_that_loses_its_compare_and_set_is_reapplied_inside_the_pass() {
    let (fx, metrics, effect) = ready_tool_ticket().await;
    // The pass's first workflow write is the durable `Dispatching` mark.
    fx.workflow_store
        .crash_at(1, CrashPoint::ConflictBeforeWrite);

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the lost mark is re-read and re-applied inside the pass");
    fx.workflow_store
        .assert_crash_fired(1, CrashPoint::ConflictBeforeWrite);
    fx.workflow_store.survive();

    assert_eq!((pass.invoked, pass.delivered), (1, 1));
    assert_eq!(fx.tools.invocation_count(TOOL), 1);
    assert_eq!(
        fleet_entry(&fx, &effect).await.status,
        AgentDispatchStatus::Completed,
        "the ticket is settled in this pass, not after a lease"
    );
    assert_eq!(write_retries(&metrics), [("dispatching".to_string(), 1.0)]);
}

#[tokio::test]
async fn a_success_record_that_loses_its_compare_and_set_is_reapplied_inside_the_pass() {
    let (fx, metrics, effect) = ready_tool_ticket().await;
    // The `Dispatching` mark, the delivery's scheduling of the next model
    // call, then the success record.
    fx.workflow_store
        .crash_at(3, CrashPoint::ConflictBeforeWrite);

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the lost success record is re-read and re-applied inside the pass");
    fx.workflow_store
        .assert_crash_fired(3, CrashPoint::ConflictBeforeWrite);
    fx.workflow_store.survive();

    assert_eq!((pass.invoked, pass.delivered), (1, 1));
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "the retry re-applies the record; it never re-invokes"
    );
    assert_eq!(
        fleet_entry(&fx, &effect).await.status,
        AgentDispatchStatus::Completed
    );
    assert_eq!(write_retries(&metrics), [("succeeded".to_string(), 1.0)]);
}

#[tokio::test]
async fn a_failure_record_that_loses_its_compare_and_set_is_reapplied_inside_the_pass() {
    let (fx, metrics, effect) = ready_tool_ticket().await;
    let _scripted = fx
        .tools
        .clone()
        .with_failure(TOOL, "card-declined", "the card was declined");
    // The `Dispatching` mark, then the failed attempt's record.
    fx.workflow_store
        .crash_at(2, CrashPoint::ConflictBeforeWrite);

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the lost failure record is re-read and re-applied inside the pass");
    fx.workflow_store
        .assert_crash_fired(2, CrashPoint::ConflictBeforeWrite);
    fx.workflow_store.survive();

    assert_eq!(pass.failed_attempts, 1);
    assert_eq!(
        fx.outbox_row(&effect).await.map(|row| row.status()),
        Some(OutboxStatus::Failed),
        "the attempt is recorded and a retry scheduled"
    );
    assert_eq!(
        fleet_entry(&fx, &effect).await.status,
        AgentDispatchStatus::RetryScheduled
    );
    assert_eq!(write_retries(&metrics), [("failed".to_string(), 1.0)]);
}

/// A worker that dies after delivering the result leaves its ticket
/// `Dispatching` under its lease. Delivering the only call's result closed
/// the call's turn, which drops the resolved effect, so once the lease
/// expires the ticket is re-claimed against an intent that is gone.
async fn stale_tool_ticket() -> (AuthorityFixture, InMemoryMetricsRecorder, AgentRunEffect) {
    let (fx, metrics, effect) = ready_tool_ticket().await;
    fx.probe.arm(AgentDispatchWindow::AfterResultDelivery);
    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the worker dies quietly");
    assert!(pass.died);
    fx.expire_lease();
    (fx, metrics, effect)
}

#[tokio::test]
async fn a_settlement_that_loses_its_compare_and_set_is_reapplied_inside_the_pass() {
    let (fx, metrics, effect) = stale_tool_ticket().await;
    // The re-claim's first workflow write is the stale ticket's settlement.
    fx.workflow_store
        .crash_at(1, CrashPoint::ConflictBeforeWrite);

    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the lost settlement is re-read and re-applied inside the pass");
    fx.workflow_store
        .assert_crash_fired(1, CrashPoint::ConflictBeforeWrite);
    fx.workflow_store.survive();

    assert!(pass.cancelled >= 1);
    assert_eq!(
        fx.tools.invocation_count(TOOL),
        1,
        "a stale ticket is settled without invoking anything"
    );
    assert_eq!(
        fleet_entry(&fx, &effect).await.status,
        AgentDispatchStatus::Cancelled
    );
    assert_eq!(write_retries(&metrics), [("cancelled".to_string(), 1.0)]);
}

/// Two tool calls in one turn: the turn stays open, holding the first
/// call's resolved effect, until the second call's result arrives.
fn two_tools_then_proposal() -> DeterministicModelAdapter {
    let call = |id: &str| {
        AgentToolCallRequest::new(
            AgentToolCallId::new(id).expect("the call id is valid"),
            AgentToolId::new(TOOL).expect("the tool id is valid"),
            serde_json::json!({ "amount": 42 }),
        )
        .expect("the tool call is bounded")
    };
    DeterministicModelAdapter::new()
        .with_turn_for(
            1,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
                .with_tool_call(call("call-1"))
                .with_tool_call(call("call-2")),
        )
        .with_turn_for(
            2,
            AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_proposal(
                AgentTaskContent::inline(serde_json::json!({ "answer": "charged" }))
                    .expect("the proposal is inline-bounded"),
            ),
        )
}

/// Issue #81 §8: a cancelled ticket's settle reason is readable from
/// durable state, so an operator can tell a ticket whose intent had already
/// resolved from one whose intent was gone.
#[tokio::test]
async fn a_cancelled_tickets_settle_reason_is_readable_from_the_fleet_index() {
    // Re-claimed while the run still holds the resolved effect: the first of
    // two calls resolved, and the turn waits on the second.
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let fx = AuthorityFixture::new(
        two_tools_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let first = fx.effect_at(1).await.expect("the first call is committed");
    fx.probe.arm(AgentDispatchWindow::AfterResultDelivery);
    let pass = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the worker dies quietly");
    assert!(pass.died);
    assert!(
        fx.effect_at(1)
            .await
            .is_some_and(|effect| effect.status.is_resolved()),
        "the open turn still holds the first call's resolved effect"
    );
    fx.expire_lease();
    fx.pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the re-claim settles the ticket");
    let entry = fleet_entry(&fx, &first).await;
    assert_eq!(entry.status, AgentDispatchStatus::Cancelled);
    assert_eq!(
        entry.last_error_code.as_deref(),
        Some("intent-already-resolved")
    );

    // Re-claimed after the closing turn was recorded, which drops the
    // resolved effect with its turn.
    let (fx, _metrics, effect) = stale_tool_ticket().await;
    assert!(
        fx.effect_at(1).await.is_none(),
        "recording the turn dropped the resolved effect"
    );
    fx.pipeline()
        .pump_run(&run_scope())
        .await
        .expect("the re-claim settles the ticket");
    let entry = fleet_entry(&fx, &effect).await;
    assert_eq!(entry.status, AgentDispatchStatus::Cancelled);
    assert_eq!(entry.last_error_code.as_deref(), Some("ticket-stale"));
}

#[tokio::test]
async fn a_store_that_loses_every_compare_and_set_exhausts_the_bound_and_fails_the_pass() {
    let (fx, metrics, effect) = ready_tool_ticket().await;
    // A second writer moves the record before every write, so no
    // re-application can ever win.
    fx.workflow_store.conflict_from(1);

    let error = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect_err("a write that never wins fails the pass, as before the retry existed");
    let attempted = fx.workflow_store.writes();
    fx.workflow_store.survive();

    assert_eq!(error.code(), "revision-conflict");
    assert_eq!(
        attempted, 4,
        "the losing write and exactly three re-applications, then nothing more"
    );
    assert_eq!(
        write_retries(&metrics),
        [
            ("dispatching".to_string(), 1.0),
            ("dispatching".to_string(), 1.0),
            ("dispatching".to_string(), 1.0),
        ]
    );
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(
        fx.outbox_row(&effect).await.map(|row| row.status()),
        Some(OutboxStatus::Pending),
        "the lease carries the ticket, exactly as before"
    );
}

#[tokio::test]
async fn a_write_the_store_refuses_for_another_reason_is_not_retried() {
    let (fx, metrics, effect) = ready_tool_ticket().await;
    // The store fails the `Dispatching` mark outright — an outage, not a
    // lost compare-and-set — and asking again would only ask it again.
    fx.workflow_store.crash_at(1, CrashPoint::BeforeWrite);

    let error = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect_err("a store failure fails the pass");
    let attempted = fx.workflow_store.writes();
    fx.workflow_store.survive();

    assert_ne!(error.code(), "revision-conflict");
    assert_eq!(attempted, 1, "only a lost compare-and-set is re-applied");
    assert!(write_retries(&metrics).is_empty());
    assert_eq!(fx.tools.invocation_count(TOOL), 0);
    assert_eq!(
        fx.outbox_row(&effect).await.map(|row| row.status()),
        Some(OutboxStatus::Pending)
    );
}

/// A cancelled run holding a `Ready` tool effect whose ticket never reached
/// the outbox: the delivery that committed it lost its flush, and the
/// cancellation arrived before anything re-drove one. This is the case the
/// wind-down fence plants a tombstone for — it schedules the ticket, then
/// cancels it, so a laggard flush lands on a terminal row.
async fn cancelled_run_with_an_unflushed_ticket(
) -> (AuthorityFixture, InMemoryMetricsRecorder, AgentRunEffect) {
    let spec = AgentEffectSpec::idempotent(3).expect("the spec is valid");
    let metrics = InMemoryMetricsRecorder::new();
    let fx = AuthorityFixture::new(
        tool_then_proposal(),
        AgentToolAuthority::new(tool_registry_for_spec(TOOL, &spec)),
        None,
    )
    .with_metrics(Arc::new(metrics.clone()));
    fx.start().await;
    fx.settle().await;

    // The model call's pass: its `Dispatching` mark, then the delivery's
    // flush of the tool ticket the model asked for — which the store loses.
    fx.workflow_store.crash_at(2, CrashPoint::BeforeWrite);
    let _lost = fx
        .pipeline()
        .pump_run(&run_scope())
        .await
        .expect_err("the delivery's flush is lost");
    fx.workflow_store
        .assert_crash_fired(2, CrashPoint::BeforeWrite);
    fx.workflow_store.survive();
    let effect = fx.effect_at(1).await.expect("the tool effect is committed");
    assert_eq!(effect.status, AgentRunEffectStatus::Ready);
    assert!(
        fx.outbox_row(&effect).await.is_none(),
        "the ticket never reached the outbox"
    );

    let mut run = fx.fx.run();
    run.recover(fx.fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::Cancel {
            operation_id: AgentOperationId::new(
                AgentOperationKind::Cancellation,
                [TENANT, AGENT, "1"],
            )
            .expect("the operation id derives"),
            reason: "operator stopped the work".to_string(),
        },
        &fx.fx.router,
        fx.fx.now(),
    )
    .await
    .expect("the cancellation applies");
    assert!(
        fx.outbox_row(&effect).await.is_none(),
        "the cancellation dispatches nothing new"
    );
    (fx, metrics, effect)
}

#[tokio::test]
async fn a_fence_tombstone_that_loses_its_compare_and_set_is_reapplied_inside_the_pass() {
    // The tombstone's two writes, in order: the ticket scheduled, then
    // cancelled.
    for (nth, operation) in [(1, "scheduled"), (2, "cancelled")] {
        let (fx, metrics, effect) = cancelled_run_with_an_unflushed_ticket().await;
        fx.workflow_store
            .crash_at(nth, CrashPoint::ConflictBeforeWrite);

        let pass = fx
            .pipeline()
            .pump_run(&run_scope())
            .await
            .unwrap_or_else(|error| {
                panic!("the lost {operation} write is re-applied inside the pass: {error}")
            });
        fx.workflow_store
            .assert_crash_fired(nth, CrashPoint::ConflictBeforeWrite);
        fx.workflow_store.survive();

        assert_eq!(pass.cancelled, 1, "{operation}");
        assert_eq!(
            fx.outbox_row(&effect).await.map(|row| row.status()),
            Some(OutboxStatus::Cancelled),
            "{operation}: the tombstone is a terminal row"
        );
        assert_eq!(fx.tools.invocation_count(TOOL), 0, "{operation}");
        assert_eq!(
            write_retries(&metrics),
            [(operation.to_string(), 1.0)],
            "{operation}"
        );
    }
}
