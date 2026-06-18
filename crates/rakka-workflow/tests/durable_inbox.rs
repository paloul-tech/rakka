//! Durable workflow inbox integration tests.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use rakka_persistence::{DurableStateStore, InMemoryDurableStateStore, Revision};
use rakka_workflow::{
    DurableInbox, InboxAcceptance, InboxCommand, InboxStatus, ManualWorkflowClock,
    OutboxAcceptance, OutboxCommand, OutboxDispatchFuture, OutboxDispatchResult, OutboxDispatcher,
    OutboxEntry, OutboxMessageId, OutboxStatus, OutboxTarget, RetryJitter, RetryPolicy,
    WorkflowError, WorkflowId, WorkflowMessageId, WorkflowState, WorkflowTelemetryEvent,
    WorkflowTimestamp,
};

#[derive(Debug, Default)]
struct RecordingDispatcher {
    results: VecDeque<OutboxDispatchResult>,
    dispatched: Vec<OutboxMessageId>,
}

impl RecordingDispatcher {
    fn with_results(results: impl IntoIterator<Item = OutboxDispatchResult>) -> Self {
        Self {
            results: VecDeque::from_iter(results),
            dispatched: Vec::new(),
        }
    }

    fn dispatched(&self) -> &[OutboxMessageId] {
        &self.dispatched
    }
}

impl OutboxDispatcher for RecordingDispatcher {
    fn dispatch<'a>(&'a mut self, entry: &'a OutboxEntry) -> OutboxDispatchFuture<'a> {
        self.dispatched.push(entry.message_id().clone());
        let result = self
            .results
            .pop_front()
            .unwrap_or(OutboxDispatchResult::Success);
        Box::pin(async move { result })
    }
}

#[tokio::test]
async fn durable_inbox_accepts_command_and_recovers_after_restart() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100));
    let workflow_id = WorkflowId::new("workflow-a");
    let mut inbox = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock.clone());

    let recovered = inbox.recover().await.expect("workflow should recover");
    assert!(recovered.inbox().is_empty());
    assert_eq!(inbox.revision().unwrap(), Revision::INITIAL);

    let accepted = inbox
        .accept(
            InboxCommand::new("message-1", "rakka.test.Command", b"payload".to_vec())
                .deduplication_key("dedup-1"),
        )
        .await
        .expect("command should be accepted");

    assert!(accepted.is_accepted());
    assert_eq!(accepted.revision(), Revision::new(1));
    assert_eq!(
        accepted.entry().accepted_at(),
        WorkflowTimestamp::from_millis(100)
    );

    let mut restarted = DurableInbox::with_clock(workflow_id, store, clock);
    let recovered = restarted
        .recover()
        .await
        .expect("workflow should recover accepted inbox entry");
    let entry = recovered
        .inbox_entry(&WorkflowMessageId::new("message-1"))
        .expect("accepted entry should be durable");
    assert_eq!(entry.payload(), b"payload");
    assert_eq!(entry.status(), InboxStatus::Pending);
}

#[tokio::test]
async fn duplicate_deduplication_key_does_not_create_duplicate_inbox_work() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(200));
    let workflow_id = WorkflowId::new("workflow-dedup");
    let mut inbox = DurableInbox::with_clock(workflow_id, store, clock);
    inbox.recover().await.unwrap();

    let first = inbox
        .accept(
            InboxCommand::new("message-1", "Command", b"first".to_vec())
                .deduplication_key("same-key"),
        )
        .await
        .expect("first command should be accepted");
    let duplicate = inbox
        .accept(
            InboxCommand::new("message-2", "Command", b"second".to_vec())
                .deduplication_key("same-key"),
        )
        .await
        .expect("duplicate should be detected");

    assert!(matches!(first, InboxAcceptance::Accepted { .. }));
    assert!(matches!(duplicate, InboxAcceptance::Duplicate { .. }));
    assert_eq!(
        duplicate.entry().message_id(),
        &WorkflowMessageId::new("message-1")
    );
    assert_eq!(duplicate.revision(), Revision::new(1));
    assert_eq!(inbox.state().unwrap().inbox().len(), 1);
}

#[tokio::test]
async fn inbox_status_transition_is_persisted_before_next_command() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(300));
    let workflow_id = WorkflowId::new("workflow-transition");
    let mut inbox = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock.clone());
    inbox.recover().await.unwrap();

    inbox
        .accept(InboxCommand::new("message-1", "Command", b"first".to_vec()))
        .await
        .unwrap();
    clock.advance_millis(10);
    let transitioned = inbox
        .transition_inbox(
            &WorkflowMessageId::new("message-1"),
            InboxStatus::Processing,
        )
        .await
        .expect("transition should persist");

    assert_eq!(transitioned.status(), InboxStatus::Processing);
    assert_eq!(inbox.revision().unwrap(), Revision::new(2));
    let persisted = store
        .load(&workflow_id.persistence_id())
        .await
        .unwrap()
        .expect("workflow state should be persisted");
    assert_eq!(persisted.revision, Revision::new(2));
    assert_eq!(
        persisted
            .state
            .inbox_entry(&WorkflowMessageId::new("message-1"))
            .unwrap()
            .status(),
        InboxStatus::Processing
    );

    inbox
        .accept(InboxCommand::new(
            "message-2",
            "Command",
            b"second".to_vec(),
        ))
        .await
        .unwrap();
    let persisted = store
        .load(&workflow_id.persistence_id())
        .await
        .unwrap()
        .expect("workflow state should remain persisted");
    assert_eq!(persisted.revision, Revision::new(3));
    assert_eq!(
        persisted
            .state
            .inbox_entry(&WorkflowMessageId::new("message-1"))
            .unwrap()
            .status(),
        InboxStatus::Processing
    );
    assert_eq!(
        persisted
            .state
            .inbox_entry(&WorkflowMessageId::new("message-2"))
            .unwrap()
            .status(),
        InboxStatus::Pending
    );
}

#[tokio::test]
async fn revision_conflict_surfaces_as_workflow_failure() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(400));
    let workflow_id = WorkflowId::new("workflow-conflict");
    let mut first = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock.clone());
    let mut stale = DurableInbox::with_clock(workflow_id.clone(), store, clock);
    first.recover().await.unwrap();
    stale.recover().await.unwrap();

    first
        .accept(InboxCommand::new("message-1", "Command", b"first".to_vec()))
        .await
        .expect("first writer should persist");
    let error = stale
        .accept(InboxCommand::new(
            "message-2",
            "Command",
            b"second".to_vec(),
        ))
        .await
        .expect_err("stale writer should hit revision conflict");

    assert!(matches!(
        error,
        WorkflowError::RevisionConflict {
            workflow_id: id,
            expected,
            actual,
        } if id == workflow_id && expected == Revision::INITIAL && actual == Revision::new(1)
    ));
}

#[tokio::test]
async fn durable_outbox_recovers_due_entry_and_dispatches_success() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(500));
    let workflow_id = WorkflowId::new("workflow-outbox-success");
    let mut workflow = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock.clone());
    workflow.recover().await.unwrap();

    let scheduled = workflow
        .schedule_outbox(OutboxCommand::new(
            "out-1",
            OutboxTarget::application("email"),
            "rakka.test.SendEmail",
            b"send".to_vec(),
        ))
        .await
        .expect("outbox command should be scheduled");

    assert!(scheduled.is_scheduled());
    assert_eq!(scheduled.entry().status(), OutboxStatus::Pending);

    let mut restarted = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock);
    restarted
        .recover()
        .await
        .expect("workflow should recover pending outbox");
    let due = restarted.due_outbox().unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].target(), &OutboxTarget::application("email"));

    let mut dispatcher = RecordingDispatcher::with_results([OutboxDispatchResult::Success]);
    let events = restarted
        .dispatch_due_outbox(&mut dispatcher)
        .await
        .expect("due outbox should dispatch");

    assert_eq!(dispatcher.dispatched(), &[OutboxMessageId::new("out-1")]);
    assert_eq!(
        events,
        vec![WorkflowTelemetryEvent::OutboxDispatchSucceeded {
            message_id: OutboxMessageId::new("out-1"),
            at: WorkflowTimestamp::from_millis(500),
        }]
    );
    let persisted = store
        .load(&workflow_id.persistence_id())
        .await
        .unwrap()
        .expect("workflow state should be persisted");
    let entry = persisted
        .state
        .outbox_entry(&OutboxMessageId::new("out-1"))
        .unwrap();
    assert_eq!(entry.status(), OutboxStatus::Dispatched);
    assert!(restarted.due_outbox().unwrap().is_empty());
}

#[tokio::test]
async fn outbox_command_scheduled_at_controls_due_discovery() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(500));
    let workflow_id = WorkflowId::new("workflow-outbox-scheduled-at");
    let mut workflow = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock.clone());
    workflow.recover().await.unwrap();

    let scheduled = workflow
        .schedule_outbox(
            OutboxCommand::new(
                "out-scheduled-at",
                OutboxTarget::application("email"),
                "rakka.test.SendEmail",
                b"send".to_vec(),
            )
            .scheduled_at(WorkflowTimestamp::from_millis(750)),
        )
        .await
        .expect("future outbox command should be scheduled");

    assert!(scheduled.is_scheduled());
    assert_eq!(
        scheduled.entry().scheduled_at(),
        WorkflowTimestamp::from_millis(750)
    );
    assert!(workflow.due_outbox().unwrap().is_empty());

    let mut restarted = DurableInbox::with_clock(workflow_id, store, clock.clone());
    restarted
        .recover()
        .await
        .expect("workflow should recover future outbox");
    assert!(restarted.due_outbox().unwrap().is_empty());

    clock.set(WorkflowTimestamp::from_millis(750));
    let due = restarted.due_outbox().unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(
        due[0].message_id(),
        &OutboxMessageId::new("out-scheduled-at")
    );
}

#[tokio::test]
async fn outbox_failure_schedules_retry_and_respects_due_time() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(600));
    let workflow_id = WorkflowId::new("workflow-outbox-retry");
    let retry_policy = RetryPolicy::new(3, 100, 1_000)
        .multiplier(2)
        .jitter(RetryJitter::FixedMillis(5));
    let mut workflow = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock.clone());
    workflow.recover().await.unwrap();
    workflow
        .schedule_outbox(
            OutboxCommand::new(
                "out-retry",
                OutboxTarget::entity("Counter", "one"),
                "Increment",
                b"1".to_vec(),
            )
            .retry_policy(retry_policy),
        )
        .await
        .unwrap();

    let mut dispatcher =
        RecordingDispatcher::with_results([OutboxDispatchResult::failure("first failure")]);
    let events = workflow.dispatch_due_outbox(&mut dispatcher).await.unwrap();

    assert_eq!(
        events,
        vec![WorkflowTelemetryEvent::OutboxDispatchRetried {
            message_id: OutboxMessageId::new("out-retry"),
            attempt: 1,
            next_retry_at: WorkflowTimestamp::from_millis(705),
            message: "first failure".to_string(),
        }]
    );
    assert!(workflow.due_outbox().unwrap().is_empty());
    let persisted = store
        .load(&workflow_id.persistence_id())
        .await
        .unwrap()
        .unwrap();
    let entry = persisted
        .state
        .outbox_entry(&OutboxMessageId::new("out-retry"))
        .unwrap();
    assert_eq!(entry.status(), OutboxStatus::Failed);
    assert_eq!(entry.attempts().attempts(), 1);
    assert_eq!(
        entry.attempts().next_retry_at(),
        Some(WorkflowTimestamp::from_millis(705))
    );
    assert_eq!(entry.attempts().last_error(), Some("first failure"));

    clock.advance_millis(105);
    let due = workflow.due_outbox().unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].message_id(), &OutboxMessageId::new("out-retry"));

    let mut dispatcher = RecordingDispatcher::with_results([OutboxDispatchResult::Success]);
    let events = workflow.dispatch_due_outbox(&mut dispatcher).await.unwrap();
    assert_eq!(
        events,
        vec![WorkflowTelemetryEvent::OutboxDispatchSucceeded {
            message_id: OutboxMessageId::new("out-retry"),
            at: WorkflowTimestamp::from_millis(705),
        }]
    );
}

#[tokio::test]
async fn outbox_timeout_emits_retry_telemetry() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(700));
    let workflow_id = WorkflowId::new("workflow-outbox-timeout");
    let mut workflow = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock);
    workflow.recover().await.unwrap();
    workflow
        .schedule_outbox(
            OutboxCommand::new(
                "out-timeout",
                OutboxTarget::actor("/system/worker"),
                "Work",
                b"payload".to_vec(),
            )
            .retry_policy(RetryPolicy::new(2, 50, 500)),
        )
        .await
        .unwrap();

    let mut dispatcher =
        RecordingDispatcher::with_results([OutboxDispatchResult::timeout("dispatch timeout")]);
    let events = workflow.dispatch_due_outbox(&mut dispatcher).await.unwrap();

    assert_eq!(
        events,
        vec![WorkflowTelemetryEvent::OutboxDispatchTimedOut {
            message_id: OutboxMessageId::new("out-timeout"),
            attempt: 1,
            next_retry_at: Some(WorkflowTimestamp::from_millis(750)),
            message: "dispatch timeout".to_string(),
        }]
    );
    let persisted = store
        .load(&workflow_id.persistence_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted
            .state
            .outbox_entry(&OutboxMessageId::new("out-timeout"))
            .unwrap()
            .status(),
        OutboxStatus::Failed
    );
}

#[tokio::test]
async fn exhausted_retry_becomes_observable_durable_state() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(800));
    let workflow_id = WorkflowId::new("workflow-outbox-exhausted");
    let mut workflow = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock.clone());
    workflow.recover().await.unwrap();
    workflow
        .schedule_outbox(
            OutboxCommand::new(
                "out-exhausted",
                OutboxTarget::application("payments"),
                "Charge",
                b"payload".to_vec(),
            )
            .retry_policy(RetryPolicy::new(1, 10, 10)),
        )
        .await
        .unwrap();

    let mut dispatcher =
        RecordingDispatcher::with_results([OutboxDispatchResult::failure("card declined")]);
    let events = workflow.dispatch_due_outbox(&mut dispatcher).await.unwrap();

    assert_eq!(
        events,
        vec![WorkflowTelemetryEvent::OutboxDispatchExhausted {
            message_id: OutboxMessageId::new("out-exhausted"),
            attempts: 1,
            message: "card declined".to_string(),
        }]
    );
    let persisted = store
        .load(&workflow_id.persistence_id())
        .await
        .unwrap()
        .expect("workflow state should be persisted");
    let entry = persisted
        .state
        .outbox_entry(&OutboxMessageId::new("out-exhausted"))
        .unwrap();
    assert_eq!(entry.status(), OutboxStatus::Exhausted);
    assert_eq!(entry.attempts().attempts(), 1);
    assert_eq!(entry.attempts().max_attempts_value(), Some(1));
    assert_eq!(entry.attempts().next_retry_at(), None);
    assert_eq!(entry.attempts().last_error(), Some("card declined"));

    clock.advance_millis(1_000);
    assert!(workflow.due_outbox().unwrap().is_empty());
}

#[tokio::test]
async fn duplicate_outbound_deduplication_key_does_not_create_duplicate_work() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(900));
    let workflow_id = WorkflowId::new("workflow-outbox-dedup");
    let mut workflow = DurableInbox::with_clock(workflow_id, store, clock);
    workflow.recover().await.unwrap();

    let first = workflow
        .schedule_outbox(
            OutboxCommand::new(
                "out-original",
                OutboxTarget::application("audit"),
                "Audit",
                b"first".to_vec(),
            )
            .deduplication_key("effect-key"),
        )
        .await
        .unwrap();
    let duplicate = workflow
        .schedule_outbox(
            OutboxCommand::new(
                "out-duplicate",
                OutboxTarget::application("audit"),
                "Audit",
                b"second".to_vec(),
            )
            .deduplication_key("effect-key"),
        )
        .await
        .unwrap();

    assert!(matches!(first, OutboxAcceptance::Scheduled { .. }));
    assert!(matches!(duplicate, OutboxAcceptance::Duplicate { .. }));
    assert_eq!(
        duplicate.entry().message_id(),
        &OutboxMessageId::new("out-original")
    );
    assert_eq!(duplicate.revision(), Revision::new(1));
    assert_eq!(workflow.state().unwrap().outbox().len(), 1);
}

struct InspectingDispatcher {
    store: InMemoryDurableStateStore<WorkflowState>,
    workflow_id: WorkflowId,
    observed_status: Arc<Mutex<Option<OutboxStatus>>>,
}

impl OutboxDispatcher for InspectingDispatcher {
    fn dispatch<'a>(&'a mut self, entry: &'a OutboxEntry) -> OutboxDispatchFuture<'a> {
        let store = self.store.clone();
        let persistence_id = self.workflow_id.persistence_id();
        let message_id = entry.message_id().clone();
        let observed_status = self.observed_status.clone();
        Box::pin(async move {
            let persisted = store
                .load(&persistence_id)
                .await
                .unwrap()
                .expect("workflow state should be persisted before dispatch");
            let status = persisted
                .state
                .outbox_entry(&message_id)
                .expect("dispatching outbox entry should be persisted")
                .status();
            *observed_status.lock().unwrap() = Some(status);
            OutboxDispatchResult::Success
        })
    }
}

#[tokio::test]
async fn outbox_dispatching_status_is_persisted_before_external_effect() {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = ManualWorkflowClock::new(WorkflowTimestamp::from_millis(1_000));
    let workflow_id = WorkflowId::new("workflow-outbox-ordering");
    let observed_status = Arc::new(Mutex::new(None));
    let mut workflow = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock);
    workflow.recover().await.unwrap();
    workflow
        .schedule_outbox(OutboxCommand::new(
            "out-ordering",
            OutboxTarget::application("side-effect"),
            "Effect",
            b"payload".to_vec(),
        ))
        .await
        .unwrap();

    let mut dispatcher = InspectingDispatcher {
        store: store.clone(),
        workflow_id: workflow_id.clone(),
        observed_status: observed_status.clone(),
    };
    workflow.dispatch_due_outbox(&mut dispatcher).await.unwrap();

    assert_eq!(
        *observed_status.lock().unwrap(),
        Some(OutboxStatus::Dispatching)
    );
    workflow
        .accept(InboxCommand::new(
            "message-after-effect",
            "Next",
            b"next".to_vec(),
        ))
        .await
        .expect("next command should be accepted after outbox persistence");
    let persisted = store
        .load(&workflow_id.persistence_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        persisted
            .state
            .outbox_entry(&OutboxMessageId::new("out-ordering"))
            .unwrap()
            .status(),
        OutboxStatus::Dispatched
    );
    assert_eq!(
        persisted
            .state
            .inbox_entry(&WorkflowMessageId::new("message-after-effect"))
            .unwrap()
            .status(),
        InboxStatus::Pending
    );
}
