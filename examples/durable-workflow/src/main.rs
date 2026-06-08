#![forbid(unsafe_code)]

//! Minimal durable workflow example with inbox, outbox, retry, deduplication, and recovery.

use std::collections::BTreeMap;

use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{
    DurableInbox, InboxCommand, OutboxCommand, OutboxDispatchFuture, OutboxDispatchResult,
    OutboxDispatcher, OutboxEntry, OutboxTarget, RetryPolicy, WorkflowId, WorkflowState,
    WorkflowTelemetryEvent, WorkflowTimestamp,
};

struct ExampleDispatcher {
    attempts: BTreeMap<String, u32>,
}

impl ExampleDispatcher {
    fn new() -> Self {
        Self {
            attempts: BTreeMap::new(),
        }
    }
}

impl OutboxDispatcher for ExampleDispatcher {
    fn dispatch<'a>(&'a mut self, entry: &'a OutboxEntry) -> OutboxDispatchFuture<'a> {
        let message_id = entry.message_id().as_str().to_string();
        let attempts = self.attempts.entry(message_id.clone()).or_default();
        *attempts += 1;
        let result = if message_id == "email-confirmation" && *attempts == 1 {
            OutboxDispatchResult::failure("temporary smtp outage")
        } else {
            OutboxDispatchResult::Success
        };
        Box::pin(async move { result })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = InMemoryDurableStateStore::<WorkflowState>::new();
    let clock = rakka_workflow::ManualWorkflowClock::new(WorkflowTimestamp::from_millis(1_000));
    let workflow_id = WorkflowId::new("checkout-42");
    let mut workflow = DurableInbox::with_clock(workflow_id.clone(), store.clone(), clock.clone());
    workflow.recover().await?;

    let accepted = workflow
        .accept(
            InboxCommand::new("checkout-command-1", "CheckoutStarted", b"cart=42".to_vec())
                .deduplication_key("checkout:42"),
        )
        .await?;
    let duplicate_inbox = workflow
        .accept(
            InboxCommand::new(
                "checkout-command-duplicate",
                "CheckoutStarted",
                b"cart=42".to_vec(),
            )
            .deduplication_key("checkout:42"),
        )
        .await?;
    workflow
        .schedule_outbox(
            OutboxCommand::new(
                "email-confirmation",
                OutboxTarget::application("email"),
                "SendEmail",
                b"order confirmed".to_vec(),
            )
            .deduplication_key("email:checkout:42")
            .retry_policy(RetryPolicy::new(3, 100, 1_000)),
        )
        .await?;
    let duplicate_outbox = workflow
        .schedule_outbox(
            OutboxCommand::new(
                "email-confirmation-duplicate",
                OutboxTarget::application("email"),
                "SendEmail",
                b"order confirmed again".to_vec(),
            )
            .deduplication_key("email:checkout:42"),
        )
        .await?;

    let mut recovered = DurableInbox::with_clock(workflow_id.clone(), store, clock.clone());
    recovered.recover().await?;
    let pending_inbox = recovered.recoverable_inbox()?.len();
    let pending_outbox = recovered.due_outbox()?.len();

    let mut dispatcher = ExampleDispatcher::new();
    let first_dispatch = recovered.dispatch_due_outbox(&mut dispatcher).await?;
    clock.advance_millis(100);
    let second_dispatch = recovered.dispatch_due_outbox(&mut dispatcher).await?;

    println!(
        "Accepted inbox work at revision {}; duplicate inbox reused message {}.",
        accepted.revision(),
        duplicate_inbox.entry().message_id()
    );
    println!("Recovered {pending_inbox} inbox item(s) and {pending_outbox} due outbox item(s).");
    println!(
        "Duplicate outbox reused message {}.",
        duplicate_outbox.entry().message_id()
    );
    println!("First dispatch: {}", describe_event(&first_dispatch[0]));
    println!("Second dispatch: {}", describe_event(&second_dispatch[0]));
    println!(
        "Workflow revision after recovery dispatch: {}.",
        recovered.revision()?
    );

    Ok(())
}

fn describe_event(event: &WorkflowTelemetryEvent) -> String {
    match event {
        WorkflowTelemetryEvent::OutboxDispatchSucceeded { message_id, .. } => {
            format!("{message_id} succeeded")
        }
        WorkflowTelemetryEvent::OutboxDispatchRetried {
            message_id,
            attempt,
            next_retry_at,
            message,
        } => format!(
            "{message_id} failed on attempt {attempt} with {message}; retry at {}",
            next_retry_at.as_millis()
        ),
        WorkflowTelemetryEvent::OutboxDispatchTimedOut {
            message_id,
            attempt,
            next_retry_at,
            message,
        } => format!(
            "{message_id} timed out on attempt {attempt} with {message}; next retry {next_retry_at:?}"
        ),
        WorkflowTelemetryEvent::OutboxDispatchExhausted {
            message_id,
            attempts,
            message,
        } => format!("{message_id} exhausted after {attempts} attempts with {message}"),
    }
}
