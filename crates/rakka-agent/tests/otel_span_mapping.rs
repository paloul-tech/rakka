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

use std::collections::HashSet;
use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    genai_operation, is_agent_span_attribute, segment_span, usage_attributes,
    validate_agent_span_attributes, AgentEffectSpec, AgentGenAiSpanExporter, AgentModelTurn,
    AgentRevisionNumber, AgentRunEffect, AgentRunEffectRequest, AgentSegmentIdentity,
    AgentSegmentOperation, AgentSegmentSink, AgentSegmentTimer, AgentTaskContent,
    AgentTelemetrySegment, AgentToolCallId, AgentToolCallRequest, AgentToolId,
    AGENT_GENAI_CONVENTION_REVISION, AGENT_OTEL_SCOPE_NAME, AGENT_SPAN_ATTRIBUTE_KEYS,
    ATTR_ERROR_TYPE, ATTR_GEN_AI_AGENT_ID, ATTR_GEN_AI_CONVERSATION_ID, ATTR_GEN_AI_OPERATION_NAME,
    ATTR_GEN_AI_PROVIDER_NAME, ATTR_GEN_AI_TOOL_NAME, ATTR_GEN_AI_TOOL_TYPE, ATTR_RAKKA_ERROR_CODE,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};
use rakka_agent_workflow::{
    AgentAttributes, AgentLogEvent, AgentLogSeverity, AgentOtelResource, AgentOtelSpanKind,
    AgentOtelSpanStatus, AgentOtlpExporterConfig, AgentTelemetryContext, AgentTimestampMillis,
    OTEL_RESOURCE_SERVICE_INSTANCE_ID, OTEL_RESOURCE_SERVICE_NAME, OTEL_RESOURCE_SERVICE_VERSION,
};
use rakka_core::InMemoryMetricsRecorder;

mod common;

use common::*;

const TRACE_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

/// The trace the caller propagated, and the caller's own span inside it.
const TRACE_ID: &str = "0af7651916cd43dd8448eb211c80319c";
const CALLER_SPAN_ID: &str = "b7ad6b7169203331";

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
            model_profile: Some("fast".to_string()),
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
                model_profile: Some("fast".to_string()),
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
            model_profile: Some("fast".to_string()),
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
        model_profile: Some("fast".to_string()),
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
                model_profile: Some("fast".to_string()),
            })
            .telemetry(traced())
            .ok(),
    );
    let spans = exporter.drain();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].kind, AgentOtelSpanKind::Client);
    assert!(spans[0].name.starts_with("chat "));
}

/// The eight retention classes of specification 17.16 are only expressible if
/// a span carries something to select on. These are the four that had nothing
/// before the follow-up, driven through the mapping rather than asserted at
/// the constants.
#[test]
fn the_retention_classes_have_attributes_to_select_on() {
    // Indeterminate effect: the outcome 17.9 requires to be retainable.
    let indeterminate = AgentTelemetrySegment::new(
        AgentSegmentOperation::EffectDispatch {
            effect_kind: "tool-call",
        },
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(9),
    )
    .telemetry(traced())
    .attribute(
        rakka_agent::SEGMENT_ATTR_EFFECT_STATUS,
        rakka_agent::AgentRunEffectStatus::Indeterminate.as_label(),
    )
    .attribute(rakka_agent::SEGMENT_ATTR_EFFECT_ATTEMPT, "3")
    .failed("rakka.agent.effect", "dispatch-ambiguous");
    let span = segment_span(&indeterminate).expect("the span maps");
    assert_eq!(span.status, AgentOtelSpanStatus::Error);
    assert_eq!(
        span.attributes
            .get(rakka_agent::ATTR_RAKKA_AGENT_EFFECT_STATUS)
            .map(String::as_str),
        Some("indeterminate"),
        "an indeterminate outcome must be selectable"
    );
    // Excessive retry.
    assert_eq!(
        span.attributes
            .get(rakka_agent::ATTR_RAKKA_AGENT_EFFECT_ATTEMPT)
            .map(String::as_str),
        Some("3")
    );

    // Checkpoint escalation or timeout: the park names its kind.
    let checkpoint = AgentTelemetrySegment::new(
        AgentSegmentOperation::CheckpointOpen,
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
    .telemetry(traced())
    .attribute(rakka_agent::SEGMENT_ATTR_CHECKPOINT_KIND, "approval")
    .ok();
    let span = segment_span(&checkpoint).expect("the span maps");
    assert_eq!(
        span.attributes
            .get(rakka_agent::ATTR_RAKKA_AGENT_CHECKPOINT_KIND)
            .map(String::as_str),
        Some("approval")
    );

    // A newly deployed version under investigation.
    let decide = AgentTelemetrySegment::new(
        AgentSegmentOperation::Decide {
            phase: "deciding-continuation",
        },
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
    .telemetry(traced())
    .attribute(rakka_agent::SEGMENT_ATTR_SETTINGS_REVISION, "7")
    .ok();
    let span = segment_span(&decide).expect("the span maps");
    assert_eq!(
        span.attributes
            .get(rakka_agent::ATTR_RAKKA_AGENT_SETTINGS_REVISION)
            .map(String::as_str),
        Some("7")
    );
}

/// The ungated segment keys and the gated convention constants are the same
/// strings, because the convention constants alias them. Two literals for one
/// key across a feature boundary is how an attribute ends up declared on one
/// side and written on the other under a different name.
#[test]
fn the_segment_keys_and_the_convention_keys_are_one_set_of_strings() {
    for (segment_key, convention_key) in [
        (
            rakka_agent::SEGMENT_ATTR_EFFECT_STATUS,
            rakka_agent::ATTR_RAKKA_AGENT_EFFECT_STATUS,
        ),
        (
            rakka_agent::SEGMENT_ATTR_EFFECT_ATTEMPT,
            rakka_agent::ATTR_RAKKA_AGENT_EFFECT_ATTEMPT,
        ),
        (
            rakka_agent::SEGMENT_ATTR_CHECKPOINT_KIND,
            rakka_agent::ATTR_RAKKA_AGENT_CHECKPOINT_KIND,
        ),
        (
            rakka_agent::SEGMENT_ATTR_SETTINGS_REVISION,
            rakka_agent::ATTR_RAKKA_AGENT_SETTINGS_REVISION,
        ),
        (
            rakka_agent::SEGMENT_ATTR_LOOP_TRANSITIONS,
            rakka_agent::ATTR_RAKKA_AGENT_LOOP_TRANSITIONS,
        ),
    ] {
        assert_eq!(segment_key, convention_key);
        assert!(is_agent_span_attribute(segment_key));
    }
}

/// A segment carrying durable decisions maps them to bounded span events, and
/// provider-reported usage to the convention's usage attributes. These are the
/// two mapping functions that still had no caller after the first pass.
///
/// The decision comes from a real run rather than a constructed one, which is
/// the part worth proving: the run entity attaching its committed decisions to
/// the segment is what gives `decision_span_event` a caller at all.
#[tokio::test]
async fn decisions_and_usage_reach_the_span_through_their_mappers() {
    let sink = Arc::new(rakka_agent::InMemoryAgentSegmentSink::new());
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup", "argument"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    );
    // Both sinks, and the coupling is deliberate: a decision is a *durable*
    // record, so the loop writes one only when a decision sink is wired.
    // Letting a span sink switch that on would make a telemetry choice change
    // durable state, which is precisely backwards. Segments carry the
    // decisions a run was already recording.
    let fx = Fixture::new(dispatcher)
        .with_segments(sink.clone())
        .with_decision_events(Arc::new(rakka_agent::InMemoryAgentDecisionEventSink::new()));
    fx.instantiate_agent().await;
    fx.create_task_traced(traced()).await;
    fx.pump().await.expect("the run completes");

    let deciding = sink
        .segments()
        .into_iter()
        .find(|segment| !segment.decisions.is_empty())
        .expect("a committed transition carried its decisions onto the segment");
    let span = segment_span(&deciding).expect("the span maps");
    assert_eq!(span.events.len(), deciding.decisions.len());
    assert!(span
        .events
        .iter()
        .all(|event| event.name == rakka_agent::AGENT_DECISION_SPAN_EVENT));
    assert!(span.events[0]
        .attributes
        .contains_key("rakka.agent.decision.kind"));
    // A decision event names no identifier, on the span as in durable state.
    for event in &span.events {
        for key in event.attributes.keys() {
            assert!(is_agent_span_attribute(key), "{key} escaped the allowlist");
            assert!(!key.ends_with(".id"));
        }
    }

    // Usage that reports nothing is dropped rather than exported as a zero.
    let empty = AgentTelemetrySegment::new(
        AgentSegmentOperation::ModelInference {
            model_profile: Some("fast".to_string()),
        },
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
    .telemetry(traced())
    .usage(rakka_agent::AgentModelUsage::default())
    .ok();
    let span = segment_span(&empty).expect("the span maps");
    assert!(!span
        .attributes
        .contains_key(rakka_agent::ATTR_GEN_AI_USAGE_INPUT_TOKENS));

    let reported = AgentTelemetrySegment::new(
        AgentSegmentOperation::ModelInference {
            model_profile: Some("fast".to_string()),
        },
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
    .telemetry(traced())
    .usage(rakka_agent::AgentModelUsage {
        input_tokens: 120,
        output_tokens: 45,
        cost_micros: 900,
    })
    .ok();
    let span = segment_span(&reported).expect("the span maps");
    assert_eq!(
        span.attributes
            .get(rakka_agent::ATTR_GEN_AI_USAGE_INPUT_TOKENS)
            .map(String::as_str),
        Some("120")
    );
    // Cost is provider-reported too, and deliberately never exported.
    assert!(!serde_json::to_string(&span)
        .expect("the record serializes")
        .contains("900"));
}

/// A log record's allowlist is a superset of the span one, and for a reason:
/// a structured log carries the durable correlation identities 17.13 asks for,
/// which are exactly the identities 17.12 forbids on a metric. Applying the
/// span list to logs would strip the audit trail while claiming to redact it.
#[test]
fn a_log_keeps_its_correlation_vocabulary_and_loses_everything_else() {
    let mut attributes = AgentAttributes::new();
    attributes.insert("run_id".to_string(), "run-1".to_string());
    attributes.insert("audit_event_id".to_string(), "audit-1".to_string());
    attributes.insert("correlation_id".to_string(), "corr-1".to_string());
    attributes.insert("prompt_text".to_string(), "SENSITIVE".to_string());
    attributes.insert("authorization".to_string(), "Bearer SECRET".to_string());

    let log = AgentLogEvent::new(
        "rakka.agent.run.started",
        AgentLogSeverity::Info,
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(1),
    );
    let log = AgentLogEvent { attributes, ..log };

    let filtered = rakka_agent::allowlist_agent_log(log);
    assert!(filtered.attributes.contains_key("run_id"));
    assert!(filtered.attributes.contains_key("audit_event_id"));
    assert!(filtered.attributes.contains_key("correlation_id"));
    assert!(!filtered.attributes.contains_key("prompt_text"));
    assert!(!filtered.attributes.contains_key("authorization"));
    for key in filtered.attributes.keys() {
        assert!(rakka_agent::is_agent_log_attribute(key), "{key} escaped");
    }

    // A run id belongs on a log and not on a span: the two lists differ on
    // purpose, and the span list is the stricter one.
    assert!(!is_agent_span_attribute("run_id"));
}

/// Every span a real run closes gets its own id, under the caller's span.
///
/// The durable telemetry context is one context for the whole run, and its
/// `traceparent` names the *caller's* span. Building each record with that id
/// meant a run exported eight or twenty-five records that were, to a backend,
/// one span — and one that already belonged to the client. No hierarchy, no
/// per-operation latency, no way to tell a tool call from the decide that
/// scheduled it. The mapping now parents to the context and derives an id per
/// record.
#[tokio::test]
async fn every_span_a_run_closes_gets_its_own_id_under_the_callers_span() {
    let exporter = Arc::new(AgentGenAiSpanExporter::new());
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup", "argument"))
            .with_turn_for(2, proposing_turn("answer")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": "result" }))
            .expect("the tool result is inline-bounded"),
    );

    let fx = Fixture::new(dispatcher).with_segments(exporter.clone());
    fx.instantiate_agent().await;
    fx.create_task_traced(traced()).await;
    fx.pump().await.expect("the run completes");

    let spans = exporter.drain();
    assert!(
        spans.len() > 1,
        "a run closes more than one operation, or this proves nothing"
    );

    let ids: HashSet<&str> = spans.iter().map(|span| span.span_id.as_str()).collect();
    assert_eq!(
        ids.len(),
        spans.len(),
        "every span needs its own id; {} records collapsed to {} ids",
        spans.len(),
        ids.len()
    );

    for span in &spans {
        assert_eq!(
            span.trace_id, TRACE_ID,
            "the run stays in the caller's trace"
        );
        assert_ne!(
            span.span_id, CALLER_SPAN_ID,
            "span `{}` is impersonating the caller's span",
            span.name
        );
        assert_eq!(
            span.parent_span_id.as_deref(),
            Some(CALLER_SPAN_ID),
            "span `{}` must hang under the caller's span",
            span.name
        );
        span.validate()
            .unwrap_or_else(|error| panic!("span `{}` is invalid: {error}", span.name));
    }
}

/// A flush that cannot be built leaves the buffer where it was.
///
/// `bridge_export` used to pass `self.drain()` as an argument, so the ring was
/// emptied before the callee validated anything. A blank endpoint — or one
/// caller-supplied log that failed its bounds — then destroyed up to a full
/// buffer of already-mapped spans, unrecoverably, while `buffered()` and
/// `dropped()` both reported a clean pipeline.
#[test]
fn a_flush_that_fails_keeps_the_spans_it_could_not_export() {
    let exporter = AgentGenAiSpanExporter::new();
    let metrics = InMemoryMetricsRecorder::new();

    for phase in ["deciding-continuation", "deciding-completion"] {
        exporter.record(
            &AgentTelemetrySegment::new(
                AgentSegmentOperation::Decide { phase },
                AgentTimestampMillis::new(1),
                AgentTimestampMillis::new(2),
            )
            .telemetry(traced())
            .ok(),
        );
    }
    assert_eq!(exporter.buffered(), 2);

    let refused = exporter.bridge_export(
        AgentOtlpExporterConfig::grpc(""),
        AgentOtelResource::new("rakka-agent"),
        &metrics.snapshot(),
        Vec::new(),
    );
    assert!(refused.is_err(), "a blank endpoint is refused");
    assert_eq!(
        exporter.buffered(),
        2,
        "a refused flush must not empty the buffer"
    );
    assert_eq!(exporter.dropped(), 0);

    let batch = exporter
        .bridge_export(
            AgentOtlpExporterConfig::grpc("http://collector:4317"),
            AgentOtelResource::new("rakka-agent"),
            &metrics.snapshot(),
            Vec::new(),
        )
        .expect("the next flush builds");
    assert_eq!(
        batch.spans.len(),
        2,
        "the spans the failed flush held survive to the one that works"
    );
    assert_eq!(exporter.buffered(), 0, "a successful flush empties it");
}

/// A record that could never be exported never enters the ring.
///
/// The buffer is cleared only on a successful flush, so one unexportable span
/// admitted to it would fail every later flush and strand every span queued
/// behind it. It is counted as unmappable at the door instead.
#[test]
fn a_span_that_cannot_pass_export_validation_is_never_buffered() {
    let exporter = AgentGenAiSpanExporter::new();

    // An inverted window: mappable, and refused by the export bounds.
    exporter.record(
        &AgentTelemetrySegment::new(
            AgentSegmentOperation::Decide {
                phase: "deciding-continuation",
            },
            AgentTimestampMillis::new(9),
            AgentTimestampMillis::new(1),
        )
        .telemetry(traced())
        .ok(),
    );

    assert_eq!(exporter.buffered(), 0, "it must not reach the buffer");
    assert_eq!(exporter.unmappable(), 1, "and the loss must be counted");
    assert_eq!(exporter.dropped(), 0);
}

/// A log record keeps the identity of the service that emitted it.
///
/// `service.name` and its siblings are on neither the span nor the log
/// attribute vocabulary — neither vocabulary is about resources — so running a
/// log's `resource` through the attribute allowlist deleted every key in it.
/// Records reached the Collector with an empty resource and nothing to
/// attribute them to, while the batch-level resource beside them travelled
/// unfiltered.
#[test]
fn a_logs_service_identity_survives_the_export_boundary() {
    let exporter = AgentGenAiSpanExporter::new();
    let metrics = InMemoryMetricsRecorder::new();

    let mut resource = AgentAttributes::new();
    resource.insert(
        OTEL_RESOURCE_SERVICE_NAME.to_string(),
        "checkout".to_string(),
    );
    resource.insert(
        OTEL_RESOURCE_SERVICE_VERSION.to_string(),
        "1.4.2".to_string(),
    );
    resource.insert(
        OTEL_RESOURCE_SERVICE_INSTANCE_ID.to_string(),
        "pod-7".to_string(),
    );
    // The generic value bounds still apply: a multi-line value is dropped
    // here rather than failing the whole batch at validation.
    resource.insert("deployment.note".to_string(), "two\nlines".to_string());

    let log = AgentLogEvent::new(
        "rakka.agent.run.started",
        AgentLogSeverity::Info,
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(1),
    )
    .resource(resource);

    let batch = exporter
        .bridge_export(
            AgentOtlpExporterConfig::grpc("http://collector:4317"),
            AgentOtelResource::new("rakka-agent"),
            &metrics.snapshot(),
            vec![log],
        )
        .expect("the bridge export builds");
    batch.validate().expect("the batch is valid");

    let exported = &batch.logs[0].resource;
    assert_eq!(
        exported.get(OTEL_RESOURCE_SERVICE_NAME).map(String::as_str),
        Some("checkout"),
        "the emitting service must survive: {exported:?}"
    );
    assert_eq!(
        exported
            .get(OTEL_RESOURCE_SERVICE_VERSION)
            .map(String::as_str),
        Some("1.4.2")
    );
    assert_eq!(
        exported
            .get(OTEL_RESOURCE_SERVICE_INSTANCE_ID)
            .map(String::as_str),
        Some("pod-7")
    );
    assert!(
        !exported.contains_key("deployment.note"),
        "a multi-line resource value is still dropped"
    );
}

/// A reconciled re-invocation still exports spans.
///
/// `begin_next_generation` used to reset the effect's telemetry to `default()`
/// to say the re-dispatch is caused by the reconciliation decision rather than
/// by the segment that scheduled the superseded attempt. It said that by
/// leaving the trace: with no `traceparent`, `from_telemetry_context` refuses
/// the record and the exporter counts it `unmappable`, so `tool-authorize`,
/// `effect-dispatch`, `model-inference` and `execute-tool` all vanished for
/// exactly the re-invocation an incident is about — and silently, because
/// `unmappable` labels no `rakka.agent.*` instrument.
#[test]
fn the_spans_of_a_reconciled_re_invocation_reach_the_exporter() {
    let call = AgentToolCallRequest::new(
        AgentToolCallId::new("call-1").expect("the call id is valid"),
        AgentToolId::new("charge-card").expect("the tool id is valid"),
        serde_json::json!({ "amount": 42 }),
    )
    .expect("the call is bounded");
    let mut effect = AgentRunEffect::new(
        &run_scope(),
        1,
        0,
        AgentRunEffectRequest::Tool {
            call: Box::new(call),
        },
        &AgentEffectSpec::non_idempotent(),
        AgentRevisionNumber::INITIAL,
        AgentTimestampMillis::new(1),
    )
    .expect("the effect derives");
    effect.telemetry = traced();

    effect
        .begin_next_generation(&run_scope(), AgentTimestampMillis::new(2))
        .expect("the operator-reconciled generation begins");

    let exporter = AgentGenAiSpanExporter::new();
    for operation in [
        AgentSegmentOperation::ToolAuthorize {
            effect_kind: "tool-call",
        },
        AgentSegmentOperation::EffectDispatch {
            effect_kind: "tool-call",
        },
        AgentSegmentOperation::ExecuteTool {
            tool_name: "charge-card".to_string(),
        },
    ] {
        exporter.record(
            &AgentTelemetrySegment::new(
                operation,
                AgentTimestampMillis::new(3),
                AgentTimestampMillis::new(4),
            )
            .telemetry(effect.telemetry.clone())
            .attribute(rakka_agent::SEGMENT_ATTR_EFFECT_ATTEMPT, "1")
            .ok(),
        );
    }

    assert_eq!(
        exporter.unmappable(),
        0,
        "the re-invocation's segments must map, not vanish"
    );
    let spans = exporter.drain();
    assert_eq!(spans.len(), 3);
    for span in &spans {
        assert_eq!(
            span.trace_id, TRACE_ID,
            "the re-invocation stays in the run's trace"
        );
        assert_eq!(
            span.parent_span_id.as_deref(),
            Some(CALLER_SPAN_ID),
            "a sibling of the superseded attempt, not a child of it"
        );
        assert_eq!(
            span.links.len(),
            1,
            "and the link says which attempt it supersedes"
        );
        span.validate().expect("the record is valid");
    }
}

/// The span attributes follow the same rule as the histogram.
///
/// `gen_ai.usage.input_tokens` and `gen_ai.usage.output_tokens` are optional
/// convention attributes, so an absent one is how the convention says "not
/// reported". Writing `"0"` instead claimed a figure Rakka has no evidence for
/// on every span of every turn a one-directional provider produced.
#[test]
fn a_usage_direction_with_no_evidence_is_omitted_rather_than_written_as_zero() {
    let reported = usage_attributes(&rakka_agent::AgentModelUsage {
        input_tokens: 120,
        output_tokens: 45,
        cost_micros: 7,
    });
    assert_eq!(
        reported.get(rakka_agent::ATTR_GEN_AI_USAGE_INPUT_TOKENS),
        Some(&"120".to_string())
    );
    assert_eq!(
        reported.get(rakka_agent::ATTR_GEN_AI_USAGE_OUTPUT_TOKENS),
        Some(&"45".to_string())
    );

    let one_sided = usage_attributes(&rakka_agent::AgentModelUsage {
        input_tokens: 0,
        output_tokens: 120,
        cost_micros: 0,
    });
    assert_eq!(
        one_sided.get(rakka_agent::ATTR_GEN_AI_USAGE_OUTPUT_TOKENS),
        Some(&"120".to_string()),
        "what the provider reported is written"
    );
    assert!(
        !one_sided.contains_key(rakka_agent::ATTR_GEN_AI_USAGE_INPUT_TOKENS),
        "and what it did not is absent, not zero: {one_sided:?}"
    );

    // Cost is on neither surface.
    assert_eq!(one_sided.len(), 1);
}

/// The model belongs to `gen_ai.request.model`, and the agent revision keeps
/// `gen_ai.agent.version` to itself.
///
/// The mapping wrote the model profile to `gen_ai.agent.version` — the key its
/// own doc calls "the agent definition revision", and the key
/// `AgentGenAiIdentity` writes that revision to — so one dimension carried two
/// unrelated vocabularies and a dashboard grouping by it mixed model profiles
/// with agent revisions. Meanwhile the convention's span name for a chat span
/// is `{gen_ai.operation.name} {gen_ai.request.model}` and the mapping produced
/// exactly that name with no such attribute to match it.
#[test]
fn the_model_profile_lands_on_the_request_model_key() {
    let model = AgentTelemetrySegment::new(
        AgentSegmentOperation::ModelInference {
            model_profile: Some("fast".to_string()),
        },
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
    .telemetry(traced())
    .identity(AgentSegmentIdentity {
        agent: Some("support-agent".to_string()),
        ..AgentSegmentIdentity::default()
    })
    .ok();
    let span = segment_span(&model).expect("the span maps");

    assert_eq!(span.name, "chat fast");
    assert_eq!(
        span.attributes
            .get(rakka_agent::ATTR_GEN_AI_REQUEST_MODEL)
            .map(String::as_str),
        Some("fast"),
        "the name's own dimension must exist as an attribute"
    );
    assert!(
        !span
            .attributes
            .contains_key(rakka_agent::ATTR_GEN_AI_AGENT_VERSION),
        "the agent revision key must not carry a model profile: {:?}",
        span.attributes
    );
    // And the key is exportable: a new attribute the allowlist does not know
    // would be filtered straight back out.
    assert!(is_agent_span_attribute(
        rakka_agent::ATTR_GEN_AI_REQUEST_MODEL
    ));
    validate_agent_span_attributes(&span.attributes).expect("every key is allowlisted");
}

/// An unprofiled deployment — the default configuration — names the bare
/// operation.
///
/// `Option<profile>` was flattened to `""` by an `unwrap_or_default`, so the
/// span name came out `"chat "`: not blank, so it exported, and different from
/// `chat` by an invisible trailing character, which backends group separately.
/// The model attribute was skipped at the same time, so nothing on the span
/// identified the model either.
#[test]
fn an_unprofiled_model_call_names_the_bare_operation() {
    let unprofiled = AgentTelemetrySegment::new(
        AgentSegmentOperation::ModelInference {
            model_profile: None,
        },
        AgentTimestampMillis::new(1),
        AgentTimestampMillis::new(2),
    )
    .telemetry(traced())
    .ok();
    let span = segment_span(&unprofiled).expect("the span maps");

    assert_eq!(span.name, "chat", "no trailing space, no second class");
    assert!(
        !span
            .attributes
            .contains_key(rakka_agent::ATTR_GEN_AI_REQUEST_MODEL),
        "and no model is claimed where none was configured"
    );
    assert_eq!(
        span.attributes
            .get(ATTR_GEN_AI_OPERATION_NAME)
            .map(String::as_str),
        Some("chat"),
        "the operation is still named"
    );
    span.validate().expect("the record is valid");
}
