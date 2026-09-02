//! The export walk: one real sharded run, exported over OTLP, asserted on the
//! decoded protobuf an OTLP receiver was handed — scenario 25 of section 18
//! re-proven on the wire.

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

    // One flush of everything the run produced — logs included. The sweep at
    // 10/12 reads the decoded payload of all three signals, and shipping an
    // always-empty logs vector made a third of that claim vacuous: it swept a
    // formatted `[]` and would have printed the same line with the record
    // redaction removed entirely.
    let bridge = export
        .bridge(
            &world.spans,
            &world.metrics.snapshot(),
            vec![walk_log_event()],
        )
        .expect("the bridge export builds");
    let span_batch = export.span_batch(&bridge.spans);
    let outcome = export.ship(&bridge, &world.exemplars).await;
    export
        .shutdown()
        .await
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
    // The exact surface, not a floor. `exported.len() >= 7` passes just as
    // happily when a recording site has been silenced by a missing
    // `with_metrics` as when one has been added, which is how the delivery
    // path came to record nothing at all. Naming them means a site that stops
    // recording fails here with the instrument it lost.
    let names: BTreeSet<&str> = exported.iter().map(|metric| metric.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "rakka.agent.decisions",
            "rakka.agent.effect.outcomes",
            "rakka.agent.effect.outstanding.duration",
            "rakka.agent.recovery.duration",
            "rakka.agent.recovery.events",
            "rakka.agent.run.transitions",
            "rakka.agent.turn.duration",
        ]
        .into_iter()
        .collect::<BTreeSet<&str>>(),
        "the exported metric surface is not the one this walk records"
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
    // An exemplar that a backend drops carries no link, so the link is not the
    // whole claim: the sample has to be a value the distribution could have
    // produced, at an instant inside the window it was collected in. Both were
    // wrong at once — the value was the point's *cumulative* `sum`, which after
    // a few observations sits past the `+Inf` edge of its own histogram, and
    // the time came from the agent domain's logical clock, dating every
    // exemplar to 1970 while its data point opened at `SystemTime::now()`.
    for (name, histogram) in &histograms {
        for point in &histogram.data_points {
            for exemplar in &point.exemplars {
                let value = match exemplar.value {
                    Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(
                        value,
                    )) => value,
                    other => panic!("{name} exported an exemplar with no double value: {other:?}"),
                };
                let sum = point.sum.unwrap_or_default();
                assert!(
                    (value * point.count as f64 - sum).abs() < 1e-9,
                    "{name}'s exemplar must be a value from the distribution, not its \
                     running total: {value} against sum {sum} over {} observations",
                    point.count
                );
                assert!(
                    exemplar.time_unix_nano >= point.start_time_unix_nano
                        && exemplar.time_unix_nano <= point.time_unix_nano,
                    "{name}'s exemplar is dated outside the window it was collected in, \
                     so a backend would drop it: {} not in {}..={}",
                    exemplar.time_unix_nano,
                    point.start_time_unix_nano,
                    point.time_unix_nano
                );
            }
        }
    }
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
    //
    // The log half of the sweep is only a claim if a log record arrived, and
    // only a claim about the *allowlist* if that record was carrying something
    // the allowlist has to remove. Both are asserted here, before the sweep
    // reads the same payload.
    let wire_logs: Vec<_> = received
        .logs()
        .iter()
        .flat_map(|resource| resource.scope_logs.iter())
        .flat_map(|scope| scope.log_records.iter())
        .cloned()
        .collect();
    assert_eq!(
        wire_logs.len(),
        1,
        "the walk's log record reached the receiver, so the sweep covers logs"
    );
    let kept: Vec<&str> = wire_logs[0]
        .attributes
        .iter()
        .map(|attribute| attribute.key.as_str())
        .collect();
    assert_eq!(
        kept,
        vec![rakka_agent_workflow::AGENT_LOG_ATTR_CORRELATION_ID],
        "the log allowlist kept the correlation id and dropped the key carrying content"
    );

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
    // And the record itself, not only what it put on the wire. The resolved
    // exporter configuration rides `AgentOtlpBridgeExport`, which is `Debug`
    // and `Serialize` by design — being serializable and application-sent is
    // the record's entire purpose — so a credential surviving in either is a
    // leak that no amount of wire redaction reaches. The sweep above read the
    // decoded payload only, and passed while the bearer token sat one field
    // away in the record that produced it.
    assert!(
        !format!("{bridge:?}").contains(EXPORTER_CREDENTIAL),
        "the exporter credential survives the bridge record's Debug"
    );
    assert!(
        !serde_json::to_string(&bridge)
            .expect("the bridge record serializes")
            .contains(EXPORTER_CREDENTIAL),
        "the exporter credential survives serializing the bridge record"
    );
    lines[9] = "ok 10/12 no prompt, tool argument, result, or exporter credential appears in \
                the decoded OTLP payload, the bridge record's Debug, or its serialization"
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
    // The durable decision record of specification 17.7, and the reason this
    // count is here rather than a `!is_empty()`. Every one of this run's four
    // deciding transitions is committed by the *delivery* — that is what
    // delivering a model result is — so an unwired delivery leaves exactly the
    // one decision the sharded registration commits on its own and silently
    // drops the other three. `rakka.agent.decisions` still appears in the
    // exported surface either way, which is why the metric-name assertion
    // above cannot see this: the sink's contents are the only witness.
    let decisions = world.decisions.events(&run_scope).len();
    assert_eq!(
        decisions, 4,
        "every deciding transition of the run is durably recorded; a delivery \
         wired for segments and metrics but not decisions records only the first"
    );
    lines[10] = format!(
        "ok 11/12 the run completed, the tool ran exactly once, and all {decisions} deciding \
         transitions were durably recorded: telemetry changed no durable outcome"
    );

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

/// The one durable log record the walk ships, so 10/12 sweeps all three signals.
///
/// It carries exactly one attribute of each kind the allowlist decides
/// between: `correlation_id` is on the log vocabulary and must survive to the
/// wire, and `prompt.text` is not — it holds a content sentinel, so a record
/// that reached the receiver with it is the leak scenario 25 forbids.
fn walk_log_event() -> rakka_agent_workflow::AgentLogEvent {
    rakka_agent_workflow::AgentLogEvent::new(
        "rakka.agent.run.transition",
        rakka_agent_workflow::AgentLogSeverity::Info,
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
    .attribute(
        rakka_agent_workflow::AGENT_LOG_ATTR_CORRELATION_ID,
        "walk-1",
    )
    .attribute("prompt.text", CONTENT_SENTINELS[0])
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
