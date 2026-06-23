#![forbid(unsafe_code)]

//! Standalone minimal local agent workflow example.
//!
//! This example mirrors the Phase 1.4 integration test, but is meant to be run
//! from a terminal with:
//!
//! ```text
//! cargo run -p rakka-example-minimal-local-agent-workflow
//! ```
//!
//! It shows the smallest useful agent workflow path:
//!
//! - define and register an agent workflow;
//! - build a first-class `StartRun` command with durability metadata;
//! - accept the command through `AgentRunInbox`, which persists it via
//!   `rakka-workflow::DurableInbox`;
//! - create a fresh facade to demonstrate recovery from durable state;
//! - execute one deterministic step and mark the durable inbox item completed.
//!
//! This is not a production workflow engine yet. The deterministic runner below
//! is intentionally tiny so the durable command boundary remains easy to see.

use std::error::Error;
use std::sync::Arc;

use rakka_agent_workflow::{
    AgentCausationId, AgentCommand, AgentCommandId, AgentCommandKind, AgentCommandMetadata,
    AgentCorrelationId, AgentDeduplicationKey, AgentDurabilityMetadata, AgentPayloadDescriptor,
    AgentRunId, AgentRunInbox, AgentRunState, AgentRunStatus, AgentStatePayload, AgentStep,
    AgentStepId, AgentStepKind, AgentTenantId, AgentTimestampMillis, AgentWorkflow,
    AgentWorkflowId, AgentWorkflowRegistry, InlineState, StateSchemaVersion,
    WorkflowDefinitionVersion, METRIC_AGENT_INBOX_COMMANDS,
};
use rakka_core::InMemoryMetricsRecorder;
use rakka_persistence::InMemoryDurableStateStore;
use rakka_workflow::{
    InboxEntry, InboxStatus, ManualWorkflowClock, WorkflowState, WorkflowTimestamp,
};

type ExampleStore = InMemoryDurableStateStore<WorkflowState>;
type ExampleInbox = AgentRunInbox<ExampleStore, ManualWorkflowClock>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Applications register workflow definitions up front. The registry lets
    // command handlers and runners validate that incoming commands and step
    // names belong to a known workflow type/version.
    let mut registry = AgentWorkflowRegistry::new();
    let workflow = local_workflow();
    registry.register(workflow.clone())?;

    // The example uses in-memory persistence and metrics so it is easy to run
    // locally. A real deployment would use a durable store such as PostgreSQL
    // and an OpenTelemetry-backed metrics pipeline.
    let store = ExampleStore::new();
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let run_id = AgentRunId::new("run-local-1");

    // The ingress facade represents the public command boundary. Recovery must
    // happen before accepting commands because `DurableInbox` needs the latest
    // persisted revision for compare-and-set writes.
    let mut ingress = agent_inbox(run_id.clone(), store.clone(), metrics.clone(), 100);
    ingress.recover().await?;

    // `accept_command` validates the agent command, serializes the full command
    // envelope, and only returns `Accepted` after the underlying durable inbox
    // has persisted it. This is the acknowledgement boundary.
    let accepted = ingress
        .accept_command(start_command(&workflow, &run_id)?)
        .await?;
    println!(
        "Accepted StartRun as durable inbox message {} at revision {}.",
        accepted.entry().message_id(),
        accepted.revision()
    );

    // Use a brand-new facade over the same store to simulate a process restart.
    // If this process had exited after the acknowledgement above, the command
    // would still be recoverable from the durable inbox state.
    let mut recovered = agent_inbox(run_id.clone(), store.clone(), metrics.clone(), 200);
    recovered.recover().await?;
    let pending = recovered.inner().recoverable_inbox()?;
    println!(
        "Recovered {} inbox item(s) from in-memory durable state.",
        pending.len()
    );

    // This lookup mirrors what a real runner would do before interpreting a
    // command: select the registered workflow definition and construct or load
    // the run state for the target run id.
    let registered = registry
        .get("local-deterministic", &WorkflowDefinitionVersion::new("v1"))
        .ok_or_else(|| invalid_data("registered workflow was not found"))?;
    let initial_run = accepted_run_state(registered, &run_id);
    let completed =
        run_one_deterministic_step(&mut recovered, registered, initial_run, &pending[0]).await?;

    println!(
        "Executed deterministic step; run {} is {}.",
        completed.run_id,
        completed.status.as_label()
    );
    println!(
        "Completed payload: {}",
        inline_payload_text(&completed.state_payload)?
    );
    println!(
        "Recoverable inbox items after completion: {}.",
        recovered.inner().recoverable_inbox()?.len()
    );

    // AgentRunInbox records bounded command acceptance metrics. Notice that the
    // metric count is exposed here, but high-cardinality identifiers such as
    // run id and command id are intentionally not metric labels.
    let acceptance_metrics = metrics
        .snapshot()
        .observations_named(METRIC_AGENT_INBOX_COMMANDS)
        .len();
    println!("Recorded {acceptance_metrics} bounded command acceptance metric(s).");

    Ok(())
}

async fn run_one_deterministic_step(
    inbox: &mut ExampleInbox,
    workflow: &AgentWorkflow,
    mut run: AgentRunState,
    entry: &InboxEntry,
) -> Result<AgentRunState, Box<dyn Error>> {
    // AgentRunInbox persisted the serialized `AgentCommand` envelope as the
    // durable inbox payload. A runner recovers that envelope before deciding
    // which state transition to apply.
    let command: AgentCommand = serde_json::from_slice(entry.payload())?;

    // Keep the example honest by checking the command against the registered
    // workflow definition. Later runner slices can grow this into richer
    // command routing and policy checks.
    if !workflow
        .command_types
        .iter()
        .any(|kind| kind == command.type_name())
    {
        return Err(
            invalid_data(format!("unsupported command type {}", command.type_name())).into(),
        );
    }
    if command.kind != AgentCommandKind::StartRun {
        return Err(invalid_data("minimal local example only accepts StartRun").into());
    }

    // The run state carries the current durable cursor. This local example has
    // exactly one planner step, but the same field can point to waits, human
    // checkpoints, child workflows, or compensation steps in later phases.
    let step_id = run
        .current_step_id
        .clone()
        .ok_or_else(|| invalid_data("run has no current step"))?;
    let step = workflow
        .steps
        .iter()
        .find(|step| step.step_id == step_id)
        .ok_or_else(|| invalid_data(format!("step {step_id} not found")))?;

    // Mark the inbox item Processing before applying business logic. If the
    // process died here, recovery would still see the entry as recoverable.
    inbox
        .inner_mut()
        .transition_inbox(entry.message_id(), InboxStatus::Processing)
        .await?;

    // The "runner" work in this example is deliberately deterministic: no
    // model call, tool call, timer, human checkpoint, or network request.
    run.status = AgentRunStatus::Running;
    run.current_attempt = run.current_attempt.saturating_add(1);
    run.updated_at = AgentTimestampMillis::new(225);

    if step.kind != AgentStepKind::Planner {
        return Err(invalid_data(format!("unexpected step kind {:?}", step.kind)).into());
    }

    // Store small, bounded example state inline. Production prompts, tool
    // outputs, model responses, and files should usually be artifact references
    // rather than large inline payloads.
    run.state_payload = AgentStatePayload::Inline(InlineState {
        content_type: "text/plain".to_string(),
        bytes: b"deterministic-plan-complete".to_vec(),
        size_bytes: 27,
    });
    run.current_step_id = None;
    run.status = AgentRunStatus::Completed;
    run.updated_at = AgentTimestampMillis::new(250);
    run.completed_at = Some(AgentTimestampMillis::new(250));

    // Completing the inbox entry removes it from `recoverable_inbox`, so a
    // later recovery does not replay this command.
    inbox
        .inner_mut()
        .transition_inbox(entry.message_id(), InboxStatus::Completed)
        .await?;

    Ok(run)
}

fn agent_inbox(
    run_id: AgentRunId,
    store: ExampleStore,
    metrics: Arc<InMemoryMetricsRecorder>,
    now_millis: u64,
) -> ExampleInbox {
    // AgentRunInbox is the agent-specific wrapper over `DurableInbox`. The
    // manual clock keeps this example deterministic and easy to inspect.
    AgentRunInbox::with_clock_and_metrics(
        run_id,
        store,
        ManualWorkflowClock::new(WorkflowTimestamp::from_millis(now_millis)),
        metrics,
    )
}

fn local_workflow() -> AgentWorkflow {
    // Workflow metadata is designed to use bounded labels for metrics while
    // keeping high-cardinality ids in traces, logs, audit events, and storage.
    AgentWorkflow {
        workflow_id: AgentWorkflowId::new("workflow-local-deterministic"),
        workflow_type: "local-deterministic".to_string(),
        definition_version: WorkflowDefinitionVersion::new("v1"),
        state_schema_version: StateSchemaVersion::new(1),
        display_name: Some("Local deterministic workflow".to_string()),
        status_labels: vec![
            AgentRunStatus::Accepted.as_label().to_string(),
            AgentRunStatus::Running.as_label().to_string(),
            AgentRunStatus::Completed.as_label().to_string(),
        ],
        command_types: vec![AgentCommandKind::StartRun.type_name().to_string()],
        // The first real engine slice will replace this hand-written runner
        // with a proper step state machine. For now, the registered step tells
        // the example which deterministic action to execute.
        steps: vec![AgentStep {
            step_id: AgentStepId::new("plan"),
            kind: AgentStepKind::Planner,
            display_name: Some("Plan deterministically".to_string()),
            next_step_ids: Vec::new(),
            timeout_ms: Some(1_000),
            config_ref: None,
            observability_labels: Default::default(),
        }],
        payload_types: vec![
            AgentPayloadDescriptor::new("local.input").content_type("application/json")
        ],
        retry_policy_ref: None,
        timeout_policy_ref: None,
        approval_policy_ref: None,
        observability_labels: Default::default(),
    }
}

fn accepted_run_state(workflow: &AgentWorkflow, run_id: &AgentRunId) -> AgentRunState {
    // Phase 2 will persist run state as part of the durable engine. In this
    // minimal example we build it locally after recovering the StartRun command.
    AgentRunState {
        run_id: run_id.clone(),
        workflow_id: workflow.workflow_id.clone(),
        tenant: Some(AgentTenantId::new("tenant-local")),
        definition_version: workflow.definition_version.clone(),
        state_schema_version: workflow.state_schema_version,
        graph_state: None,
        status: AgentRunStatus::Accepted,
        current_step_id: Some(AgentStepId::new("plan")),
        current_attempt: 0,
        inputs_ref: None,
        state_payload: AgentStatePayload::Empty,
        checkpoints: Vec::new(),
        pending_effects: Vec::new(),
        pending_human_checkpoint: None,
        cancellation: None,
        created_at: AgentTimestampMillis::new(100),
        updated_at: AgentTimestampMillis::new(100),
        completed_at: None,
    }
}

fn start_command(
    workflow: &AgentWorkflow,
    run_id: &AgentRunId,
) -> Result<AgentCommand, Box<dyn Error>> {
    // Command ids and deduplication keys serve different purposes:
    // command_id identifies this durable inbox message, while deduplication_key
    // lets retries or duplicate HTTP requests resolve to the same durable work.
    Ok(AgentCommand::new(
        AgentCommandKind::StartRun,
        AgentCommandMetadata::new(
            workflow.workflow_id.clone(),
            run_id.clone(),
            AgentCommandId::new("command-start-local-1"),
            AgentDurabilityMetadata::new(
                AgentDeduplicationKey::new("start:run-local-1"),
                // Causation says "what caused this command"; correlation ties
                // related commands, effects, logs, and audit events together.
                AgentCausationId::new("ingress-local-1"),
                AgentCorrelationId::new("corr-local-1"),
            ),
            AgentTenantId::new("tenant-local"),
            AgentTimestampMillis::new(100),
        )?,
    )?)
}

fn inline_payload_text(payload: &AgentStatePayload) -> Result<String, Box<dyn Error>> {
    match payload {
        AgentStatePayload::Inline(state) => Ok(String::from_utf8(state.bytes.clone())?),
        _ => Err(invalid_data("expected inline state payload").into()),
    }
}

fn invalid_data(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}
