//! Agent durable inbox facade tests.

use std::sync::Arc;

use rakka_agent_workflow::{
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentInboxDuplicateReason,
    AgentInboxError, AgentRunId, AgentRunInbox, AgentTenantId, AgentTimestampMillis,
    AgentWorkflowId, METRIC_AGENT_INBOX_COMMANDS,
};
use rakka_core::{InMemoryMetricsRecorder, MetricObservation};
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{ManualWorkflowClock, WorkflowError, WorkflowState, WorkflowTimestamp};

type TestStore = InMemoryDurableStateStore<WorkflowState>;

#[tokio::test]
async fn start_run_is_accepted_after_durable_persistence() {
    let store = TestStore::new();
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let mut inbox = test_inbox(store.clone(), metrics.clone());
    inbox.recover().await.expect("inbox should recover");

    let command = start_command("command-1", "command:run-1:start");
    let accepted = inbox
        .accept_command(command.clone())
        .await
        .expect("start command should persist");

    assert!(accepted.is_accepted());
    assert_eq!(accepted.entry().message_id().as_str(), "command-1");
    assert_eq!(accepted.entry().message_type(), "agent.start-run");
    assert_eq!(
        accepted
            .entry()
            .deduplication_key()
            .map(ToString::to_string)
            .as_deref(),
        Some("command:run-1:start")
    );

    let persisted: AgentCommand =
        serde_json::from_slice(accepted.entry().payload()).expect("payload should deserialize");
    assert_eq!(persisted, command);
    assert_eq!(inbox.inner().recoverable_inbox().unwrap().len(), 1);
    assert_eq!(store.len(), 1);

    assert_metric(&metrics, "accepted", "none");
}

#[tokio::test]
async fn duplicate_start_by_message_id_returns_existing_entry() {
    let store = TestStore::new();
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let mut inbox = test_inbox(store, metrics.clone());
    inbox.recover().await.expect("inbox should recover");

    let command = start_command("command-1", "command:run-1:start");
    let accepted = inbox
        .accept_command(command.clone())
        .await
        .expect("first command should persist");
    let duplicate = inbox
        .accept_command(command)
        .await
        .expect("duplicate command should be reported");

    assert!(duplicate.is_duplicate());
    assert_eq!(
        duplicate.duplicate_reason(),
        Some(AgentInboxDuplicateReason::MessageId)
    );
    assert_eq!(
        duplicate.entry().message_id().as_str(),
        accepted.entry().message_id().as_str()
    );

    assert_metric(&metrics, "accepted", "none");
    assert_metric(&metrics, "duplicate", "message-id");
}

#[tokio::test]
async fn recovered_inbox_duplicate_by_deduplication_key_returns_existing_entry() {
    let store = TestStore::new();
    let metrics = Arc::new(InMemoryMetricsRecorder::new());

    let mut first = test_inbox(store.clone(), metrics.clone());
    first.recover().await.expect("first inbox should recover");
    first
        .accept_command(start_command("command-1", "command:run-1:start"))
        .await
        .expect("first command should persist");

    let mut recovered = test_inbox(store, metrics.clone());
    recovered
        .recover()
        .await
        .expect("second inbox should recover existing state");
    let duplicate = recovered
        .accept_command(start_command("command-2", "command:run-1:start"))
        .await
        .expect("duplicate deduplication key should be reported");

    assert!(duplicate.is_duplicate());
    assert_eq!(
        duplicate.duplicate_reason(),
        Some(AgentInboxDuplicateReason::DeduplicationKey)
    );
    assert_eq!(duplicate.entry().message_id().as_str(), "command-1");
    assert_eq!(recovered.inner().recoverable_inbox().unwrap().len(), 1);

    assert_metric(&metrics, "duplicate", "deduplication-key");
}

#[tokio::test]
async fn invalid_command_is_rejected_before_persistence() {
    let store = TestStore::new();
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let mut inbox = test_inbox(store.clone(), metrics.clone());
    inbox.recover().await.expect("inbox should recover");

    let mut command = start_command("command-1", "command:run-1:start");
    command.metadata.command_id = AgentCommandId::new("");

    let error = inbox
        .accept_command(command)
        .await
        .expect_err("invalid command should be rejected");

    match error {
        AgentInboxError::Rejected { error } => {
            assert!(error.to_string().contains("command_id"));
        }
        other => panic!("expected rejected command error, got {other:?}"),
    }
    assert_eq!(inbox.inner().recoverable_inbox().unwrap().len(), 0);
    assert_eq!(store.len(), 0);
    assert_metric(&metrics, "rejected", "none");
}

#[tokio::test]
async fn lower_level_workflow_error_is_mapped_and_metered() {
    let store = TestStore::new();
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let mut inbox = test_inbox(store, metrics.clone());

    let error = inbox
        .accept_command(start_command("command-1", "command:run-1:start"))
        .await
        .expect_err("unrecovered inbox should fail");

    match error {
        AgentInboxError::Workflow {
            error: WorkflowError::NotRecovered { workflow_id },
        } => assert_eq!(workflow_id.as_str(), "run-1"),
        other => panic!("expected mapped workflow error, got {other:?}"),
    }
    assert_eq!(error_code_for_unrecovered(), "not-recovered");
    assert_metric(&metrics, "failed", "not-recovered");
}

fn test_inbox(
    store: TestStore,
    metrics: Arc<InMemoryMetricsRecorder>,
) -> AgentRunInbox<TestStore, ManualWorkflowClock> {
    AgentRunInbox::with_clock_and_metrics(
        AgentRunId::new("run-1"),
        store,
        ManualWorkflowClock::new(WorkflowTimestamp::from_millis(100)),
        metrics,
    )
}

fn start_command(command_id: &str, deduplication_key: &str) -> AgentCommand {
    AgentCommand::new(
        AgentCommandKind::StartRun,
        AgentCommandMetadata::new(
            AgentWorkflowId::new("workflow-1"),
            AgentRunId::new("run-1"),
            AgentCommandId::new(command_id),
            AgentDurabilityMetadata::new(
                AgentDeduplicationKey::new(deduplication_key),
                AgentCausationId::new("ingress-1"),
                AgentCorrelationId::new("corr-1"),
            ),
            AgentTenantId::new("tenant-a"),
            AgentTimestampMillis::new(100),
        )
        .expect("metadata should be valid"),
    )
    .expect("command should be valid")
}

fn assert_metric(metrics: &InMemoryMetricsRecorder, expected_outcome: &str, expected_detail: &str) {
    let snapshot = metrics.snapshot();
    let observation = snapshot
        .observations_named(METRIC_AGENT_INBOX_COMMANDS)
        .into_iter()
        .find(|observation| {
            observation.attribute("outcome") == Some(expected_outcome)
                && observation.attribute("detail") == Some(expected_detail)
        })
        .expect("expected inbox metric should be recorded");

    assert_bounded_metric(observation, expected_outcome, expected_detail);
}

fn assert_bounded_metric(
    observation: &MetricObservation,
    expected_outcome: &str,
    expected_detail: &str,
) {
    assert_eq!(observation.attribute("command_type"), Some("StartRun"));
    assert_eq!(
        observation.attribute("message_type"),
        Some("agent.start-run")
    );
    assert_eq!(observation.attribute("outcome"), Some(expected_outcome));
    assert_eq!(observation.attribute("detail"), Some(expected_detail));
    assert_eq!(observation.attribute("run_id"), None);
    assert_eq!(observation.attribute("command_id"), None);
    assert_eq!(observation.attribute("deduplication_key"), None);
}

fn error_code_for_unrecovered() -> &'static str {
    WorkflowError::NotRecovered {
        workflow_id: rakka_workflow::WorkflowId::new("run-1"),
    }
    .code()
}
