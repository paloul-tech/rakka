//! Durable workflow inbox integration tests.

use rakka_persistence::{DurableStateStore, InMemoryDurableStateStore, Revision};
use rakka_workflow::{
    DurableInbox, InboxAcceptance, InboxCommand, InboxStatus, ManualWorkflowClock, WorkflowError,
    WorkflowId, WorkflowMessageId, WorkflowState, WorkflowTimestamp,
};

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
