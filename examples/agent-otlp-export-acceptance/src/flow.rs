//! The export walk: one real sharded run, exported over OTLP, asserted on the
//! decoded protobuf an OTLP receiver was handed.

use std::collections::{BTreeSet, HashMap};

use a2a::{Message, Part, PartContent, Role, SendMessageRequest};
use rakka_a2a::agents::A2AAgentTarget;
use rakka_agent::testkit::DeterministicModelAdapter;
use rakka_agent::{
    load_agent_run_state, passivate_agent_run_entity, registered_agent_entity_ref,
    registered_agent_run_entity_ref, registered_agent_task_entity_ref, run_id_for_assignment,
    AgentApprovalDecision, AgentAuthorityEnvelope, AgentCheckpointDecision, AgentDefinition,
    AgentDefinitionId, AgentEntityCommand, AgentEntityMessage, AgentEntityReply, AgentId,
    AgentModelTurn, AgentOperationId, AgentOperationKind, AgentRevisionProvenance,
    AgentRunEntityCommand, AgentRunEntityMessage, AgentRunEntityReply, AgentRunScope,
    AgentRunStatus, AgentSchemaPolicy, AgentScope, AgentSettings, AgentTaskContent,
    AgentTaskDefinition, AgentTaskDefinitionId, AgentTaskEntityMessage, AgentTaskId,
    AgentTaskResultCheck, AgentTaskResultRule, AgentTaskRuleId, AgentTaskScope, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, TenantId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentAuditEventId, AgentCausationId, AgentTimestampMillis, PrincipalRef,
};

use crate::collector::InProcessCollector;
use crate::report::{AcceptanceReport, EXPECTED_TRANSCRIPT};
use crate::sdk::AgentTelemetryExport;

use crate::wiring::{World, ASK_TIMEOUT, TOOL};

const TENANT: &str = "acme";
const AGENT: &str = "support-agent";
const INGRESS_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
/// The trace id half of [`INGRESS_PARENT`], as OTLP's 16 raw bytes.
const INGRESS_TRACE_ID: [u8; 16] = [
    0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80, 0x31, 0x9c,
];

/// The content sentinels planted in model text, tool arguments, and the
/// proposed result. Any exported record containing one has leaked content
/// default telemetry must never carry
/// ([17.14](../../../docs/plans/rakka-agent/spec.md), scenario 25) — and this
/// walk sweeps the *decoded OTLP payload*, which is the last place it could.
pub const CONTENT_SENTINELS: [&str; 3] = [
    "SENSITIVE-REASONING",
    "SECRET-TOKEN",
    "charged and resolved",
];

/// The exporter credential the walk configures, so the sweep can prove it
/// travelled as an OTLP header and reached no span, metric, or log record.
pub const EXPORTER_CREDENTIAL: &str = "RAKKA-EXPORTER-BEARER-SENTINEL";

fn tenant() -> TenantId {
    TenantId::new(TENANT)
}

fn agent_id() -> AgentId {
    AgentId::new(AGENT).expect("the agent id is valid")
}

fn agent_scope() -> AgentScope {
    AgentScope::new(tenant(), agent_id()).expect("the agent scope is valid")
}

fn provenance(at: u64) -> AgentRevisionProvenance {
    AgentRevisionProvenance {
        principal: PrincipalRef {
            principal_type: "service".to_string(),
            principal_id: "ingress".to_string(),
            display_name: None,
        },
        accepted_at: AgentTimestampMillis::new(at),
        causation_id: AgentCausationId::new(format!("cause-{at}")),
        audit_ref: AgentAuditEventId::new(format!("audit-{at}")),
    }
}

/// The public task: one deterministic result rule.
pub fn task_definition() -> AgentTaskDefinition {
    AgentTaskDefinition::new(
        AgentTaskDefinitionId::new("resolve-ticket").expect("the definition id is valid"),
        "Resolve one customer support ticket.",
        crate::wiring::schema("ticket-input"),
        crate::wiring::schema("ticket-result"),
    )
    .expect("the task definition is valid")
    .with_result_rule(AgentTaskResultRule::new(
        AgentTaskRuleId::new("answer-present").expect("the rule id is valid"),
        AgentTaskResultCheck::NonEmptyString {
            pointer: "/answer".to_string(),
        },
    ))
}

/// The two scripted model turns: ask for the gated tool, then propose.
pub fn scripted_adapter() -> DeterministicModelAdapter {
    let tool_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(format!("{} about the charge.", CONTENT_SENTINELS[0]))
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("the call id is valid"),
                AgentToolId::new(TOOL).expect("the tool id is valid"),
                serde_json::json!({ "amount": 42, "card_token": CONTENT_SENTINELS[1] }),
            )
            .expect("the tool call is bounded"),
        );
    let proposing_turn = AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text(format!("{} toward the answer.", CONTENT_SENTINELS[0]))
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": CONTENT_SENTINELS[2] }))
                .expect("the proposal is inline-bounded"),
        );
    DeterministicModelAdapter::new()
        .with_turn_for(1, tool_turn)
        .with_turn_for(2, proposing_turn)
}

fn task_message(message_id: &str) -> Message {
    let mut message = Message::new(
        Role::User,
        vec![Part {
            content: PartContent::Data(serde_json::json!({ "ticket": 1 })),
            filename: None,
            media_type: Some("application/json".to_string()),
            metadata: None,
        }],
    );
    message.message_id = message_id.to_string();
    message
}

fn send_request(message: &Message) -> SendMessageRequest {
    let mut metadata = HashMap::new();
    metadata.insert(
        "traceparent".to_string(),
        serde_json::Value::String(INGRESS_PARENT.to_string()),
    );
    SendMessageRequest {
        message: message.clone(),
        configuration: None,
        metadata: Some(metadata),
        tenant: Some(TENANT.to_string()),
    }
}

/// Runs the export walk and returns the transcript plus the typed facts.
///
/// # Panics
///
/// Panics if any line's fact does not hold — the walk is the check.
#[allow(clippy::too_many_lines)]
pub async fn run_acceptance() -> AcceptanceReport {
    let collector = InProcessCollector::start().await;
    let received = collector.received();
    let export = AgentTelemetryExport::install(
        &crate::sdk::exporter_config(collector.endpoint(), EXPORTER_CREDENTIAL),
        &crate::sdk::export_resource(),
    )
    .expect("the OTLP exporters build");
    export.install_tracing_bridge();

    let world = World::new(
        scripted_adapter(),
        A2AAgentTarget::new(agent_id(), task_definition()),
    );
    let mut lines = vec![String::new(); EXPECTED_TRANSCRIPT.len()];

    let run_scope = drive_run(&world).await;

    // One flush of everything the run produced.
    let bridge = export
        .bridge(&world.spans, &world.metrics.snapshot(), Vec::new())
        .expect("the bridge export builds");
    let span_batch = export.span_batch(&bridge.spans);
    let outcome = export.ship(&bridge, &world.exemplars).await;
    export
        .shutdown()
        .expect("the exporters drain and shut down");

    lines[0] = format!(
        "ok  1/12 one sharded run completed with the SDK, subscriber, exporter and flush \
         owned by this binary: {} spans mapped",
        span_batch.len()
    );

    // 2/12 — every class the run closed reached a convention span.
    let classes: BTreeSet<&str> = span_batch.iter().map(|span| span.name.as_ref()).collect();
    assert!(
        classes.len() >= 8,
        "a full run closes many operation classes, saw {classes:?}"
    );
    lines[1] = format!(
        "ok  2/12 {} distinct convention span names left the binary, mapped from the \
         ungated segment vocabulary",
        classes.len()
    );

    // 3/12 — every span kind the adapter can produce, including the A2A
    // ingress SERVER span that is the only one of its kind in the workspace.
    let kinds: BTreeSet<String> = span_batch
        .iter()
        .map(|span| format!("{:?}", span.span_kind))
        .collect();
    for kind in ["Server", "Client", "Producer", "Consumer", "Internal"] {
        assert!(
            kinds.contains(kind),
            "span kind {kind} never reached the exporter; saw {kinds:?}"
        );
    }
    lines[2] =
        "ok  3/12 all five span kinds exported, the A2A ingress SERVER span among them".to_string();

    // 4/12 — the pinned convention revision travels with every span.
    let scope = rakka_agent::otel::agent_instrumentation_scope();
    let schema_url = scope.schema_url.clone().expect("the scope pins a schema");
    for span in &span_batch {
        assert_eq!(
            span.instrumentation_scope.schema_url().map(str::to_string),
            Some(schema_url.clone()),
            "an exported span lost the pinned convention revision"
        );
    }
    lines[3] = format!(
        "ok  4/12 every exported span carries the pinned convention revision {}",
        rakka_agent::otel::AGENT_GENAI_CONVENTION_REVISION
    );

    // 5/12 — the ingress trace survived every durable boundary to the wire.
    let traced = span_batch
        .iter()
        .filter(|span| span.span_context.trace_id().to_bytes() == INGRESS_TRACE_ID)
        .count();
    assert_eq!(
        traced,
        span_batch.len(),
        "every span of this run belongs to the ingress trace"
    );
    let distinct: BTreeSet<_> = span_batch
        .iter()
        .map(|span| span.span_context.span_id().to_bytes())
        .collect();
    assert_eq!(
        distinct.len(),
        span_batch.len(),
        "each exported span has its own id"
    );
    lines[4] = format!(
        "ok  5/12 all {} spans joined the ingress trace, each with its own span id",
        span_batch.len()
    );

    // 6/12 — the catalogue's units reached the wire.
    let received_metrics = received.metrics();
    let exported: Vec<_> = received_metrics
        .iter()
        .flat_map(|resource| resource.scope_metrics.iter())
        .flat_map(|scope| scope.metrics.iter())
        .collect();
    assert!(
        !exported.is_empty(),
        "the OTLP receiver was handed no metrics at all"
    );
    let united = exported
        .iter()
        .filter(|metric| !metric.unit.is_empty())
        .count();
    assert_eq!(
        united,
        exported.len(),
        "every catalogued metric exports with its declared unit"
    );
    lines[5] = format!(
        "ok  6/12 {} metrics reached the OTLP receiver, every one carrying its catalogued unit",
        exported.len()
    );

    // 7/12 — histogram bucket boundaries survived, with the +Inf overflow.
    let histograms: Vec<_> = exported
        .iter()
        .filter_map(|metric| match &metric.data {
            Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Histogram(histogram)) => {
                Some((metric.name.clone(), histogram))
            }
            _ => None,
        })
        .collect();
    assert!(
        !histograms.is_empty(),
        "the run records histograms; none reached the receiver"
    );
    for (name, histogram) in &histograms {
        for point in &histogram.data_points {
            assert!(
                !point.explicit_bounds.is_empty(),
                "{name} exported without its declared bucket boundaries"
            );
            assert_eq!(
                point.bucket_counts.len(),
                point.explicit_bounds.len() + 1,
                "{name} lost its +Inf overflow bucket"
            );
        }
    }
    lines[6] = format!(
        "ok  7/12 {} histograms exported with the catalogue's boundaries and the +Inf bucket",
        histograms.len()
    );

    // 8/12 — an exemplar links each histogram to the trace that produced it.
    let with_exemplar = histograms
        .iter()
        .filter(|(_, histogram)| {
            histogram.data_points.iter().any(|point| {
                point
                    .exemplars
                    .iter()
                    .any(|exemplar| exemplar.trace_id == INGRESS_TRACE_ID)
            })
        })
        .count();
    assert!(
        with_exemplar > 0,
        "no exported histogram carried an exemplar, so 17.12's link is a promise only"
    );
    lines[7] = format!(
        "ok  8/12 {with_exemplar} of {} exported histograms carry an exemplar linking to the \
         run's trace",
        histograms.len()
    );

    // 9/12 — the receiver got the spans over a real socket.
    let wire_spans: usize = received
        .traces()
        .iter()
        .flat_map(|resource| resource.scope_spans.iter())
        .map(|scope| scope.spans.len())
        .sum();
    assert_eq!(
        wire_spans,
        span_batch.len(),
        "every mapped span reached the OTLP receiver"
    );
    assert_eq!(outcome.failed_signals, 0, "no signal failed to export");
    lines[8] =
        format!("ok  9/12 {wire_spans} spans and every metric crossed a real OTLP gRPC socket");

    // 10/12 — scenario 25 at the last boundary it could leak from.
    let payload = format!(
        "{:?}{:?}{:?}",
        received.traces(),
        received_metrics,
        received.logs()
    );
    for sentinel in CONTENT_SENTINELS {
        assert!(
            !payload.contains(sentinel),
            "the exported OTLP payload carries the content sentinel {sentinel}"
        );
    }
    assert!(
        !payload.contains(EXPORTER_CREDENTIAL),
        "the exporter credential reached an exported record"
    );
    lines[9] = "ok 10/12 no prompt, tool argument, result, or exporter credential appears \
                anywhere in the decoded OTLP payload"
        .to_string();

    // 11/12 — the durable outcome is what it would have been untraced.
    let status = run_status(&world, &run_scope).await;
    assert_eq!(
        status,
        Some(AgentRunStatus::Completed),
        "the traced, exported run still completes"
    );
    assert_eq!(
        world.tools.invocation_count(TOOL),
        1,
        "the external system was called exactly once"
    );
    lines[10] = "ok 11/12 the run completed and the tool ran exactly once: telemetry changed \
                 no durable outcome"
        .to_string();

    // 12/12 — the loss counters exist and read clean on a healthy path.
    let snapshot = world.metrics.snapshot();
    let queue = snapshot
        .observations_named(rakka_agent::METRIC_AGENT_TELEMETRY_EXPORT_QUEUE)
        .len();
    assert!(
        queue > 0,
        "the export queue depth is published on every flush"
    );
    assert_eq!(world.spans.dropped(), 0, "a healthy path drops nothing");
    // Not zero, and deliberately so. A run's *first* recovery segment closes
    // before the run has any durable state, so it carries no `traceparent`.
    // Slice 6.3a refuses to invent one — a fabricated trace id is a causal
    // claim — and counts the segment instead. `tests/acceptance.rs` proves
    // that mechanism directly; this line reports what it cost.
    let unmappable = world.spans.unmappable();
    lines[11] = format!(
        "ok 12/12 export queue depth published, nothing dropped, and {unmappable} pre-trace \
         segment counted unmappable rather than exported under an invented trace"
    );

    AcceptanceReport {
        lines,
        spans_exported: wire_spans,
        metrics_exported: exported.len(),
        histograms_with_exemplars: with_exemplar,
        span_kinds: kinds.len(),
    }
}

/// Drives the sharded run from A2A ingress to `Completed`, and returns its
/// durable scope so the assertions read the record the walk actually wrote.
///
/// Public because the exporter-failure suite drives the same run against a
/// broken export path: the claim there is that the durable outcome is
/// identical, and it can only be identical if it is the same walk.
pub async fn drive_run(world: &World) -> AgentRunScope {
    let agent = registered_agent_entity_ref(&world.agent_registration, &agent_scope());
    let mut envelope = AgentAuthorityEnvelope::empty();
    envelope
        .task_definitions
        .insert(task_definition().definition_id.clone());
    for (tool, declaration) in world.registry.tool_declarations() {
        envelope.tools.insert(tool, declaration);
    }
    let definition = AgentDefinition::new(
        AgentDefinitionId::new("support-v1").expect("the definition id is valid"),
        "Resolves customer support tickets end to end.",
        envelope,
    )
    .expect("the agent definition is valid");
    let reply = agent
        .ask(
            |reply_to| AgentEntityMessage {
                command: AgentEntityCommand::Instantiate {
                    operation_id: AgentOperationId::for_agent(
                        AgentOperationKind::DefinitionUpdate,
                        &agent_scope(),
                        "1",
                    )
                    .expect("the operation id derives"),
                    definition: Box::new(definition),
                    settings: Box::new(AgentSettings::default()),
                    provenance: Box::new(provenance(1)),
                },
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded agent replies");
    assert!(
        matches!(reply, AgentEntityReply::Applied { .. }),
        "the agent instantiates, got {reply:?}"
    );

    let message = task_message("msg-1");
    let created = world
        .service
        .send_message(&a2a_server::ServiceParams::new(), &send_request(&message))
        .await
        .expect("the A2A send is accepted");
    let task_scope = AgentTaskScope::new(
        tenant(),
        AgentTaskId::new(&created.id).expect("the task id is valid"),
    )
    .expect("the task scope is valid");
    let run_scope = run_scope_of(&task_scope);
    let task = registered_agent_task_entity_ref(&world.task_registration, &task_scope);
    let run = registered_agent_run_entity_ref(&world.run_registration, &run_scope);

    let pump = || async {
        for _round in 0..16 {
            let _settled_task = task
                .ask(
                    |reply_to| AgentTaskEntityMessage::Settle { reply_to },
                    ASK_TIMEOUT,
                )
                .await
                .expect("the sharded task settles");
            let _settled_run = run
                .ask(
                    |reply_to| AgentRunEntityMessage::Settle { reply_to },
                    ASK_TIMEOUT,
                )
                .await
                .expect("the sharded run settles");
            let _was_resident = passivate_agent_run_entity(
                &world.sharding,
                world.run_registration.key(),
                &run_scope,
            )
            .expect("run passivation routes");
            let pass = world
                .pipeline()
                .pump_run(&run_scope)
                .await
                .expect("the dispatch pass runs");
            let status = run_status(world, &run_scope).await;
            let waiting = matches!(
                status,
                Some(AgentRunStatus::WaitingForApproval)
                    | Some(AgentRunStatus::WaitingForReconciliation)
            );
            let terminal = status.is_some_and(AgentRunStatus::is_terminal);
            let moved = pass.registered + pass.claimed + pass.delivered + pass.cancelled > 0;
            if terminal || (waiting && !moved) {
                return;
            }
        }
        panic!("the export walk did not converge");
    };

    pump().await;
    assert_eq!(
        run_status(world, &run_scope).await,
        Some(AgentRunStatus::WaitingForApproval),
        "the checkpoint-required tool parks the run"
    );

    let checkpoint_id =
        load_agent_run_state(&world.runs, &run_scope, &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")
            .expect("the run exists")
            .loop_state()
            .expect("the loop exists")
            .open_checkpoints()
            .first()
            .expect("the approval checkpoint is open")
            .checkpoint_id
            .clone();
    let approved = run
        .ask(
            |reply_to| AgentRunEntityMessage::Command {
                command: Box::new(AgentRunEntityCommand::ResolveCheckpoint {
                    operation_id: AgentOperationId::for_agent(
                        AgentOperationKind::CheckpointResolution,
                        &agent_scope(),
                        "approve-1",
                    )
                    .expect("the decision key derives"),
                    checkpoint_id,
                    resolver: PrincipalRef {
                        principal_type: "user".to_string(),
                        principal_id: "approver".to_string(),
                        display_name: None,
                    },
                    decision: Box::new(AgentCheckpointDecision::Approval(
                        AgentApprovalDecision::Approve {
                            credential_binding: None,
                            expires_at: AgentTimestampMillis::new(10_000_000),
                            allowed_use_count: 1,
                        },
                    )),
                }),
                reply_to,
            },
            ASK_TIMEOUT,
        )
        .await
        .expect("the sharded run replies to the decision");
    assert!(
        matches!(approved, AgentRunEntityReply::Applied { .. }),
        "the approval applies, got {approved:?}"
    );

    pump().await;
    run_scope
}

fn run_scope_of(task_scope: &AgentTaskScope) -> AgentRunScope {
    let run = run_id_for_assignment(
        task_scope.task(),
        rakka_agent::AgentAssignmentGeneration::new(1),
    )
    .expect("the run id derives");
    AgentRunScope::new(tenant(), agent_id(), run).expect("the run scope is valid")
}

/// The run's durable status, read from the store rather than the actor.
pub async fn run_status(world: &World, run_scope: &AgentRunScope) -> Option<AgentRunStatus> {
    load_agent_run_state(&world.runs, run_scope, &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .and_then(|state| state.status())
}
