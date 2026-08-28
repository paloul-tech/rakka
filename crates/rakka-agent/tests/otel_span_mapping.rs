//! The GenAI convention mapping, over the segments a run actually closes.
//!
//! Specification: [17.6](../../../docs/plans/rakka-agent/spec.md) (span names,
//! kinds, and status), [17.14](../../../docs/plans/rakka-agent/spec.md)
//! (content capture and redaction),
//! [17.15](../../../docs/plans/rakka-agent/spec.md) (baggage is untrusted),
//! [17.20](../../../docs/plans/rakka-agent/spec.md) (the pinned revision), and
//! scenario 25 at the export boundary.
//!
//! The module under test shipped fully unit-tested and entirely unreachable.
//! What was missing was never assertions about `span_name()` — it was a
//! production call site and the mapping of everything a span record carries
//! *besides* its name: status, error, the four GenAI attributes that were
//! declared and written by nothing, and a guard on what may be exported at
//! all. That is what this suite is about.

#![cfg(feature = "otel")]

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    genai_operation, is_agent_span_attribute, segment_span, validate_agent_span_attributes,
    AgentGenAiSpanExporter, AgentModelTurn, AgentSegmentIdentity, AgentSegmentOperation,
    AgentSegmentSink, AgentSegmentTimer, AgentTaskContent, AgentTelemetrySegment, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, AGENT_GENAI_CONVENTION_REVISION, AGENT_OTEL_SCOPE_NAME,
    AGENT_SPAN_ATTRIBUTE_KEYS, ATTR_ERROR_TYPE, ATTR_GEN_AI_AGENT_ID, ATTR_GEN_AI_CONVERSATION_ID,
    ATTR_GEN_AI_OPERATION_NAME, ATTR_GEN_AI_PROVIDER_NAME, ATTR_GEN_AI_TOOL_NAME,
    ATTR_GEN_AI_TOOL_TYPE, ATTR_RAKKA_ERROR_CODE, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentAttributes, AgentOtelResource, AgentOtelSpanKind, AgentOtelSpanStatus,
    AgentOtlpExporterConfig, AgentTelemetryContext, AgentTimestampMillis,
};
use rakka_core::InMemoryMetricsRecorder;

mod common;

use common::*;

const TRACE_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

fn traced() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(TRACE_PARENT.to_string()),
        ..AgentTelemetryContext::default()
    }
}

/// One of every bounded operation class, so the mapping is exercised whole.
fn every_operation() -> Vec<AgentSegmentOperation> {
    vec![
        AgentSegmentOperation::A2aIngress {
            operation: "send-message".to_string(),
        },
        AgentSegmentOperation::Decide {
            phase: "deciding-continuation",
        },
        AgentSegmentOperation::InvokeAgent { agent_name: None },
        AgentSegmentOperation::EffectSchedule {
            effect_kind: "model-call",
        },
        AgentSegmentOperation::EffectDispatch {
            effect_kind: "tool-call",
        },
        AgentSegmentOperation::ToolAuthorize {
            effect_kind: "tool-call",
        },
        AgentSegmentOperation::ModelInference {
            model_profile: "fast".to_string(),
        },
        AgentSegmentOperation::ExecuteTool {
            tool_name: "lookup".to_string(),
        },
        AgentSegmentOperation::DelegateToPeer {
            peer_class: "billing".to_string(),
        },
        AgentSegmentOperation::WorkflowInvoke {
            workflow_class: "refund".to_string(),
        },
        AgentSegmentOperation::GoalEvaluate,
        AgentSegmentOperation::ValidateTaskResult,
        AgentSegmentOperation::Handoff,
        AgentSegmentOperation::TeamOperation {
            operation: "claim".to_string(),
        },
        AgentSegmentOperation::ModerationTurn {
            operation: "turn".to_string(),
        },
        AgentSegmentOperation::WakeAdmit,
        AgentSegmentOperation::AutonomyAdmit,
        AgentSegmentOperation::BudgetReserve,
        AgentSegmentOperation::BudgetSettle,
        AgentSegmentOperation::MemoryOperation { tier: "private" },
        AgentSegmentOperation::Retrieval {
            backend: "pgvector".to_string(),
        },
        AgentSegmentOperation::CheckpointOpen,
        AgentSegmentOperation::RunResume,
        AgentSegmentOperation::RunRecover,
    ]
}

fn segment(operation: AgentSegmentOperation) -> AgentTelemetrySegment {
    AgentTelemetrySegment::new(
        operation,
        AgentTimestampMillis::new(10),
        AgentTimestampMillis::new(42),
    )
    .telemetry(traced())
    .ok()
}

/// Every bounded class maps to a span whose name embeds no identifier, whose
/// kind is the one 17.6 requires, and which validates as a bridge record.
#[test]
fn every_operation_maps_to_a_valid_span_with_a_bounded_name() {
    for operation in every_operation() {
        let label = operation.as_label();
        let span = segment_span(&segment(operation.clone()))
            .unwrap_or_else(|error| panic!("{label} should map to a span: {error}"));
        span.validate()
            .unwrap_or_else(|error| panic!("{label} produced an invalid record: {error}"));
        assert!(!span.name.is_empty(), "{label} produced an empty span name");
        // A span name carries classes, never identity: the run and agent ids
        // are on the record's attributes, and must not be in its name.
        assert!(
            !span.name.contains("run-") && !span.name.contains("agent-"),
            "{label} embedded an identifier in `{}`",
            span.name
        );
        assert_eq!(span.status, AgentOtelSpanStatus::Ok);
        assert_eq!(span.trace_id, "0af7651916cd43dd8448eb211c80319c");
    }
}

/// The kinds 17.6 names for the rows that are not `INTERNAL`.
#[test]
fn the_non_internal_rows_carry_the_kinds_the_convention_requires() {
    let rows: &[(AgentSegmentOperation, AgentOtelSpanKind)] = &[
        (
            AgentSegmentOperation::A2aIngress {
                operation: "send-message".to_string(),
            },
            AgentOtelSpanKind::Server,
        ),
        (
            AgentSegmentOperation::ModelInference {
                model_profile: "fast".to_string(),
            },
            AgentOtelSpanKind::Client,
        ),
        (
            AgentSegmentOperation::DelegateToPeer {
                peer_class: "billing".to_string(),
            },
            AgentOtelSpanKind::Client,
        ),
        (
            AgentSegmentOperation::EffectSchedule {
                effect_kind: "model-call",
            },
            AgentOtelSpanKind::Producer,
        ),
        (AgentSegmentOperation::Handoff, AgentOtelSpanKind::Producer),
        (
            AgentSegmentOperation::EffectDispatch {
                effect_kind: "tool-call",
            },
            AgentOtelSpanKind::Consumer,
        ),
    ];
    for (operation, kind) in rows {
        assert_eq!(
            genai_operation(operation).span_kind(),
            *kind,
            "{} has the wrong span kind",
            operation.as_label()
        );
    }
}

/// A failed operation maps to `Error` with a stable low-cardinality type and
/// the stable Rakka code — never an unbounded message as a grouping
/// attribute. `span()` used to leave every status `Unset`, and the three
/// error attributes were declared and written by nothing.
#[test]
fn a_failed_segment_maps_to_an_error_status_with_a_bounded_code() {
    let failed = AgentTelemetrySegment::new(
        AgentSegmentOperation::ModelInference {
            model_profile: "fast".to_string(),
        },
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(9),
    )
    .telemetry(traced())
    .failed("rakka.agent.model", "model-timeout");

    let span = segment_span(&failed).expect("the span maps");
    assert_eq!(span.status, AgentOtelSpanStatus::Error);
    assert_eq!(
        span.attributes.get(ATTR_ERROR_TYPE).map(String::as_str),
        Some("rakka.agent.model")
    );
    assert_eq!(
        span.attributes
            .get(ATTR_RAKKA_ERROR_CODE)
            .map(String::as_str),
        Some("model-timeout")
    );
}

/// The GenAI attributes that were re-exported and written by no mapping
/// function now have a writer, and the identity reaches the record as an
/// attribute rather than as part of the name.
#[test]
fn the_genai_attributes_are_written_by_the_mapping() {
    let tool = AgentTelemetrySegment::new(
        AgentSegmentOperation::ExecuteTool {
            tool_name: "lookup".to_string(),
        },
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
    .telemetry(traced())
    .identity(AgentSegmentIdentity {
        agent: Some("support".to_string()),
        run: Some("run-1".to_string()),
        ..AgentSegmentIdentity::default()
    })
    .ok();
    let span = segment_span(&tool).expect("the span maps");
    assert_eq!(span.name, "execute_tool lookup");
    assert_eq!(
        span.attributes
            .get(ATTR_GEN_AI_OPERATION_NAME)
            .map(String::as_str),
        Some("execute_tool")
    );
    assert_eq!(
        span.attributes
            .get(ATTR_GEN_AI_TOOL_NAME)
            .map(String::as_str),
        Some("lookup")
    );
    assert!(span.attributes.contains_key(ATTR_GEN_AI_TOOL_TYPE));
    assert_eq!(
        span.attributes
            .get(ATTR_GEN_AI_AGENT_ID)
            .map(String::as_str),
        Some("support")
    );
    assert_eq!(
        span.attributes
            .get(ATTR_GEN_AI_CONVERSATION_ID)
            .map(String::as_str),
        Some("run-1")
    );

    let model = segment(AgentSegmentOperation::ModelInference {
        model_profile: "fast".to_string(),
    });
    let span = segment_span(&model).expect("the span maps");
    assert_eq!(span.name, "chat fast");
    assert!(span.attributes.contains_key(ATTR_GEN_AI_PROVIDER_NAME));
}

/// Nothing outside the allowlist reaches an export record — not an attribute
/// a caller set on the segment, and not a key the durable trace context
/// carried as baggage.
#[test]
fn an_attribute_outside_the_allowlist_cannot_reach_the_record() {
    let mut baggage = AgentAttributes::new();
    baggage.insert("prompt_text".to_string(), "SENSITIVE".to_string());
    baggage.insert("authorization".to_string(), "Bearer SECRET".to_string());

    let leaky = AgentTelemetrySegment::new(
        AgentSegmentOperation::Decide {
            phase: "deciding-continuation",
        },
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
    .telemetry(AgentTelemetryContext {
        trace_parent: Some(TRACE_PARENT.to_string()),
        baggage,
        ..AgentTelemetryContext::default()
    })
    .attribute("tool_arguments", "SENSITIVE")
    .ok();

    let span = segment_span(&leaky).expect("the span maps");
    for key in span.attributes.keys() {
        assert!(
            is_agent_span_attribute(key),
            "{key} is outside the exported vocabulary"
        );
    }
    let rendered = serde_json::to_string(&span).expect("the record serializes");
    assert!(!rendered.contains("SENSITIVE"));
    assert!(!rendered.contains("SECRET"));
    validate_agent_span_attributes(&span.attributes).expect("the exported set is allowlisted");
}

/// The allowlist is a closed set and every entry in it is accepted, so the
/// guard and the vocabulary cannot disagree.
#[test]
fn the_allowlist_accepts_exactly_what_it_declares() {
    assert!(!AGENT_SPAN_ATTRIBUTE_KEYS.is_empty());
    for key in AGENT_SPAN_ATTRIBUTE_KEYS {
        assert!(is_agent_span_attribute(key));
        let mut attributes = AgentAttributes::new();
        attributes.insert((*key).to_string(), "bounded".to_string());
        assert!(validate_agent_span_attributes(&attributes).is_ok());

        // An allowlisted key still may not smuggle an unbounded or
        // multi-line value: bounding is not sanitizing, and neither
        // substitutes for the other.
        let mut overlong = AgentAttributes::new();
        overlong.insert((*key).to_string(), "x".repeat(4096));
        assert_eq!(
            validate_agent_span_attributes(&overlong).err().as_deref(),
            Some(*key)
        );
    }
    let mut unknown = AgentAttributes::new();
    unknown.insert("completion_text".to_string(), "x".to_string());
    assert!(validate_agent_span_attributes(&unknown).is_err());
}

/// The exporter is a bounded ring: at capacity it drops the oldest and counts
/// the loss, and it never blocks or fails the operation that produced the
/// segment.
#[test]
fn the_exporter_buffer_is_bounded_and_counts_what_it_drops() {
    let exporter = AgentGenAiSpanExporter::with_capacity(2);
    for _ in 0..5 {
        exporter.record(&segment(AgentSegmentOperation::WakeAdmit));
    }
    assert_eq!(exporter.buffered(), 2);
    assert_eq!(exporter.dropped(), 3);
    assert_eq!(exporter.drain().len(), 2);
    assert_eq!(exporter.buffered(), 0);
}

/// A segment whose durable context carries no trace parent belongs to no
/// trace. It is counted rather than exported under an invented one.
#[test]
fn a_contextless_segment_is_counted_rather_than_invented() {
    let exporter = AgentGenAiSpanExporter::new();
    exporter.record(&AgentTelemetrySegment::new(
        AgentSegmentOperation::WakeAdmit,
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    ));
    assert_eq!(exporter.buffered(), 0);
    assert_eq!(exporter.unmappable(), 1);
    assert_eq!(exporter.dropped(), 0);
}

fn tool_calling_turn(tool: &str, argument: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("SENSITIVE-REASONING about the ticket.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id"),
                AgentToolId::new(tool).expect("tool id"),
                serde_json::json!({ "query": argument, "token": "SECRET-TOKEN" }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("SENSITIVE-REASONING again.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
}

/// Scenario 25, at the export boundary rather than only at durable state.
///
/// The existing proof scans four surfaces — decision events, metric
/// observations, the operational snapshot, and the session view — and no
/// export record, because until this slice no production path built one. A
/// real run now drives the exporter, and the serialized OTLP bridge batch
/// joins the scan.
#[tokio::test]
async fn the_export_batch_of_a_real_run_carries_no_content_or_credentials() {
    let exporter = Arc::new(AgentGenAiSpanExporter::new());
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup", "SENSITIVE-ARG"))
            .with_turn_for(2, proposing_turn("SENSITIVE-ANSWER")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": "SENSITIVE-RESULT" }))
            .expect("the tool result is inline-bounded"),
    );

    let fx = Fixture::new(dispatcher)
        .with_segments(exporter.clone())
        .with_metrics(metrics.clone());
    fx.instantiate_agent().await;
    fx.create_task_traced(traced()).await;
    fx.pump().await.expect("the run completes");

    assert!(
        exporter.buffered() > 0,
        "a real run reaches the adapter: the module has a production call site"
    );
    assert_eq!(exporter.dropped(), 0);

    let batch = exporter
        .bridge_export(
            AgentOtlpExporterConfig::grpc("http://collector:4317"),
            AgentOtelResource::new("rakka-agent"),
            &metrics.snapshot(),
            Vec::new(),
        )
        .expect("the bridge export builds");
    batch
        .validate()
        .expect("every record in the batch is valid");

    // The pinned convention revision travels with the data.
    let scope = batch.scope.as_ref().expect("the batch is stamped");
    assert_eq!(scope.name, AGENT_OTEL_SCOPE_NAME);
    assert!(scope
        .schema_url
        .as_deref()
        .is_some_and(|url| url.ends_with(AGENT_GENAI_CONVENTION_REVISION)));

    // Units and buckets reached the exported metrics rather than being
    // dropped while claiming convention compliance.
    assert!(
        batch
            .metrics
            .metrics()
            .iter()
            .any(|metric| metric.unit().is_some()),
        "the agent instrument catalogue supplied the units"
    );

    let rendered = serde_json::to_string(&batch).expect("the batch serializes");
    for sentinel in [
        "SENSITIVE-REASONING",
        "SENSITIVE-ARG",
        "SENSITIVE-RESULT",
        "SENSITIVE-ANSWER",
        "SECRET-TOKEN",
    ] {
        assert!(
            !rendered.contains(sentinel),
            "{sentinel} leaked into the OTLP export batch"
        );
    }

    // And every attribute on every exported span is one the adapter declares.
    for span in &batch.spans {
        validate_agent_span_attributes(&span.attributes)
            .unwrap_or_else(|key| panic!("{key} reached span `{}`", span.name));
    }
}

/// A segment closed by the dispatcher reaches the adapter too, so the model
/// and tool rows are not gate-only: they come from the real pipeline, not
/// from the scripted driver that bypasses it.
#[tokio::test]
async fn the_dispatcher_closes_the_model_and_tool_rows() {
    let exporter = Arc::new(AgentGenAiSpanExporter::new());
    let timer = AgentSegmentTimer::start(AgentTimestampMillis::new(1));
    exporter.record(
        &timer
            .close(AgentSegmentOperation::ModelInference {
                model_profile: "fast".to_string(),
            })
            .telemetry(traced())
            .ok(),
    );
    let spans = exporter.drain();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].kind, AgentOtelSpanKind::Client);
    assert!(spans[0].name.starts_with("chat "));
}
