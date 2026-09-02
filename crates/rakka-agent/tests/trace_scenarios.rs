//! The trace-context flow and the section 18 telemetry scenarios.
//!
//! Specification: sections 17.4, 17.5, 17.14, and 17.16; scenarios 22, 23,
//! 24, 25, and 26. An ingress-traced creation flows its context down the
//! whole causal chain — creation -> assignment -> run acceptance -> every
//! effect and checkpoint the loop commits — with no participant doing
//! per-exchange work, and:
//!
//! - a durable wait holds no live span (Rakka core never constructs a span
//!   object at all — it persists serializable context) and a resume links
//!   both the parked segment and its trigger (scenario 22);
//! - the persisted chain survives owner loss without changing effect
//!   behavior (scenario 23, joining the schema half in
//!   `telemetry_context.rs`);
//! - the sampled flag changes recording, never metrics, decisions, or
//!   durable execution (scenario 24);
//! - default telemetry carries no model text, tool payload, memory content,
//!   or credential (scenario 25, joining the metric half in
//!   `agent_metrics.rs`); and
//! - an unavailable sink never blocks correctness and its loss is visible
//!   through bounded counters and the authoritative snapshot (scenario 26).

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    agent_operational_snapshot, assemble_agent_session_view, load_agent_run_state,
    AgentDecisionEventSink, AgentDecisionKind, AgentModelTurn, AgentObservabilityError,
    AgentObservabilityFuture, AgentRunStatus, AgentSchemaPolicy, AgentTaskContent, AgentToolCallId,
    AgentToolCallRequest, AgentToolId, InMemoryAgentDecisionEventSink,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION, METRIC_AGENT_DECISIONS, METRIC_AGENT_EFFECT_OUTCOMES,
    METRIC_AGENT_TELEMETRY_FLUSH_FAILURES,
};
use rakka_agent_workflow::{AgentTelemetryContext, AgentTimestampMillis};
use rakka_core::InMemoryMetricsRecorder;

mod common;

use common::*;

const INGRESS_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

fn ingress_context(trace_parent: &str) -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(trace_parent.to_string()),
        ..AgentTelemetryContext::default()
    }
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
        .with_text("SENSITIVE-REASONING toward an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
}

fn full_run_dispatcher() -> ScriptedDispatcher {
    ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup", "SENSITIVE-ARG"))
            .with_turn_for(2, proposing_turn("SENSITIVE-ANSWER")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": "SENSITIVE-RESULT" }))
            .expect("the tool result is inline-bounded"),
    )
}

/// Scenario 23: the ingress context flows creation -> assignment -> run ->
/// effects, survives owner loss (a fresh store over the same durable records
/// is exactly a shard movement), and changes no effect behavior — the traced
/// run's durable outcome is identical to the untraced run's.
#[tokio::test]
async fn ingress_context_flows_to_every_effect_and_survives_owner_loss() {
    let traced = Fixture::new(full_run_dispatcher());
    traced.instantiate_agent().await;
    traced
        .create_task_traced(ingress_context(INGRESS_PARENT))
        .await;
    traced.pump().await.expect("the traced run completes");

    let untraced = Fixture::new(full_run_dispatcher());
    untraced.instantiate_agent().await;
    untraced.create_task().await;
    untraced.pump().await.expect("the untraced run completes");

    // Owner loss: a brand-new store facade over the surviving durable records.
    let state = load_agent_run_state(&traced.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    let loop_state = state.loop_state().expect("the loop exists");
    assert_eq!(
        loop_state.telemetry().trace_parent.as_deref(),
        Some(INGRESS_PARENT),
        "the run's committing segment carries the ingress chain after recovery"
    );
    for effect in loop_state.effects() {
        assert_eq!(
            effect.telemetry.trace_parent.as_deref(),
            Some(INGRESS_PARENT),
            "every committed effect was stamped from the committing segment"
        );
    }

    // Without changing effect behavior: the durable outcome is identical.
    let traced_run = state.snapshot().expect("the traced run accepted");
    let untraced_state =
        load_agent_run_state(&untraced.runs, &run_scope(), &AgentSchemaPolicy::default())
            .await
            .expect("the run state loads")
            .expect("the run exists");
    let untraced_run = untraced_state
        .snapshot()
        .expect("the untraced run accepted");
    assert_eq!(traced_run.status, AgentRunStatus::Completed);
    assert_eq!(traced_run.status, untraced_run.status);
    assert_eq!(traced_run.turn, untraced_run.turn);
    assert_eq!(traced_run.budget, untraced_run.budget);
}

/// Scenario 22: a run parked behind a checkpoint holds only serializable
/// context — the checkpoint record stores the parked span's own durable
/// identity, and the `checkpoint-open` segment exports under it — and the
/// resolution segment the runtime closes links both the parked span and the
/// incoming request's span ([specification 17.11]).
///
/// Every assertion is over a segment the runtime produced or a record the
/// runtime persisted. The test constructs no link of its own: an earlier
/// version built both links in its body and asserted on its construction,
/// which proved the helper and not the runtime, and the matrix recorded the
/// MUST as unmet for exactly that reason.
#[tokio::test]
async fn a_parked_checkpoint_carries_the_segment_a_resume_doubly_links() {
    use rakka_agent::{
        AgentApprovalDecision, AgentCheckpoint, AgentCheckpointDecision, AgentOperationId,
        AgentOperationKind, AgentRunEntityCommand, AgentSegmentOperation, InMemoryAgentSegmentSink,
        ATTR_AGENT_TELEMETRY_LINK_KIND, LINK_KIND_PARKED_CHECKPOINT, LINK_KIND_RESUME_REQUEST,
        SEGMENT_ATTR_CHECKPOINT_KIND,
    };
    use rakka_agent_workflow::PrincipalRef;

    const REQUEST_PARENT: &str = "00-1bf7651916cd43dd8448eb211c80319d-c7ad6b7169203332-01";

    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = checkpointed_fixture().with_segments(sink.clone());
    fx.instantiate_agent().await;
    fx.create_task_traced(ingress_context(INGRESS_PARENT)).await;
    fx.pump().await.expect("the run parks on its checkpoint");

    let state = load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    let loop_state = state.loop_state().expect("the loop exists");
    let checkpoint = loop_state
        .open_checkpoints()
        .first()
        .expect("the approval checkpoint is open")
        .clone();
    let effect = loop_state
        .effects()
        .iter()
        .find(|effect| effect.effect_id == checkpoint.bound_effect.effect_id)
        .expect("the gated effect is on the loop")
        .clone();

    // The parked span's identity is derived from the gated effect's context
    // and the checkpoint id — the two facts any later reader also holds.
    let parked =
        AgentCheckpoint::parked_span_identity(&effect.telemetry, &checkpoint.checkpoint_id)
            .expect("a traced effect derives a parked identity");
    assert_eq!(parked.trace_id, "0af7651916cd43dd8448eb211c80319c");
    assert_eq!(
        checkpoint.telemetry.trace_parent.as_deref(),
        Some(parked.trace_parent().as_str()),
        "the record stores the parked span's own identity, durably"
    );

    let opened: Vec<_> = sink
        .segments()
        .into_iter()
        .filter(|segment| matches!(segment.operation, AgentSegmentOperation::CheckpointOpen))
        .collect();
    assert_eq!(opened.len(), 1, "{:?}", sink.operations());
    assert_eq!(
        opened[0].span_id.as_deref(),
        Some(parked.span_id.as_str()),
        "the parked segment exports under the identity the record stores"
    );
    assert_eq!(
        opened[0].telemetry.trace_parent.as_deref(),
        Some(INGRESS_PARENT),
        "and stays a child of the ingress that activated the run"
    );

    // The human decision arrives carrying its own request span.
    let mut run = fx.run();
    run.recover(fx.now()).await.expect("the run recovers");
    run.apply(
        AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                "d1",
            )
            .expect("the decision key derives"),
            checkpoint_id: checkpoint.checkpoint_id.clone(),
            resolver: PrincipalRef {
                principal_type: "user".to_string(),
                principal_id: "approver".to_string(),
                display_name: None,
            },
            decision: Box::new(AgentCheckpointDecision::Approval(
                AgentApprovalDecision::Approve {
                    credential_binding: None,
                    expires_at: AgentTimestampMillis::new(1_000_000),
                    allowed_use_count: 1,
                },
            )),
            telemetry: ingress_context(REQUEST_PARENT),
        },
        &fx.router,
        fx.now(),
    )
    .await
    .expect("the decision applies");

    let resolved: Vec<_> = sink
        .segments()
        .into_iter()
        .filter(|segment| matches!(segment.operation, AgentSegmentOperation::CheckpointResolve))
        .collect();
    assert_eq!(resolved.len(), 1, "{:?}", sink.operations());
    let resolution = &resolved[0];
    let identity =
        AgentCheckpoint::resolve_span_identity(&effect.telemetry, &checkpoint.checkpoint_id)
            .expect("the resolve identity derives");
    assert_eq!(
        resolution.span_id.as_deref(),
        Some(identity.span_id.as_str()),
        "the resolution exports under the identity a park can name in advance"
    );
    assert_eq!(
        resolution.attributes.get(SEGMENT_ATTR_CHECKPOINT_KIND),
        Some(&"approval".to_string())
    );
    assert_eq!(
        resolution.telemetry.trace_parent.as_deref(),
        Some(INGRESS_PARENT),
        "the resolution is still a child of the run's ingress; the links say what caused it"
    );
    let link_to = |kind: &str| {
        resolution
            .telemetry
            .span_links
            .iter()
            .find(|link| {
                link.attributes.get(ATTR_AGENT_TELEMETRY_LINK_KIND) == Some(&kind.to_string())
            })
            .unwrap_or_else(|| panic!("a `{kind}` link: {:?}", resolution.telemetry.span_links))
    };
    let parked_link = link_to(LINK_KIND_PARKED_CHECKPOINT);
    assert_eq!(parked_link.trace_id, parked.trace_id);
    assert_eq!(
        parked_link.span_id, parked.span_id,
        "the resolution links the span the park exported under"
    );
    let request_link = link_to(LINK_KIND_RESUME_REQUEST);
    assert_eq!(request_link.trace_id, "1bf7651916cd43dd8448eb211c80319d");
    assert_eq!(request_link.span_id, "c7ad6b7169203332");

    // And the links survive the bridge into the export record, when the
    // bridge is built at all.
    #[cfg(feature = "otel")]
    {
        let exported = rakka_agent::segment_span(resolution).expect("the resolution maps");
        assert_eq!(exported.span_id, identity.span_id);
        assert_eq!(exported.links.len(), 2);
    }
}

/// Scenario 24: the W3C sampled flag changes trace recording downstream and
/// nothing else — durable outcomes, decision events, and metrics are
/// identical for a sampled and an unsampled ingress.
#[tokio::test]
async fn the_sampled_flag_changes_no_metric_event_or_durable_outcome() {
    async fn drive(trace_parent: &str) -> (Vec<AgentDecisionKind>, usize, usize, AgentRunStatus) {
        let sink = Arc::new(InMemoryAgentDecisionEventSink::new());
        let metrics = Arc::new(InMemoryMetricsRecorder::new());
        let fx = Fixture::new(full_run_dispatcher())
            .with_decision_events(sink.clone())
            .with_metrics(metrics.clone());
        fx.instantiate_agent().await;
        fx.create_task_traced(ingress_context(trace_parent)).await;
        fx.pump().await.expect("the run completes");
        let kinds = sink
            .events(&run_scope())
            .iter()
            .map(|event| event.kind)
            .collect();
        let snapshot = metrics.snapshot();
        let status = fx.run_snapshot().await.expect("the run exists").status;
        (
            kinds,
            snapshot.observations_named(METRIC_AGENT_DECISIONS).len(),
            snapshot
                .observations_named(METRIC_AGENT_EFFECT_OUTCOMES)
                .len(),
            status,
        )
    }

    let sampled = drive(INGRESS_PARENT).await;
    let unsampled = drive("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00").await;
    assert_eq!(sampled, unsampled);
    assert_eq!(sampled.3, AgentRunStatus::Completed);
}

/// Scenario 25: no telemetry surface — decision events, metric observations,
/// the authoritative snapshot, the session view — carries model text, tool
/// arguments, tool results, proposal content, or credential material. The
/// content lives in durable correctness state; telemetry gets bounded labels,
/// counts, and references.
#[tokio::test]
async fn default_telemetry_carries_no_content_or_credentials() {
    let sink = Arc::new(InMemoryAgentDecisionEventSink::new());
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(full_run_dispatcher())
        .with_decision_events(sink.clone())
        .with_metrics(metrics.clone());
    fx.instantiate_agent().await;
    fx.create_task_traced(ingress_context(INGRESS_PARENT)).await;
    fx.pump().await.expect("the run completes");

    let scope = run_scope();
    let mut telemetry_surfaces = Vec::new();
    telemetry_surfaces
        .push(serde_json::to_string(&sink.events(&scope)).expect("the decision events serialize"));
    telemetry_surfaces.push(format!("{:?}", metrics.snapshot().observations()));
    let snapshot = agent_operational_snapshot(
        &fx.runs,
        &scope,
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    telemetry_surfaces.push(serde_json::to_string(&snapshot).expect("the snapshot serializes"));
    let view = assemble_agent_session_view(
        &fx.runs,
        &scope,
        &AgentSchemaPolicy::default(),
        Some(sink.as_ref()),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the view assembles")
    .expect("the run exists");
    telemetry_surfaces.push(serde_json::to_string(&view).expect("the view serializes"));

    for surface in &telemetry_surfaces {
        for sentinel in [
            "SENSITIVE-REASONING",
            "SENSITIVE-ARG",
            "SENSITIVE-RESULT",
            "SENSITIVE-ANSWER",
            "SECRET-TOKEN",
        ] {
            assert!(
                !surface.contains(sentinel),
                "{sentinel} leaked into a telemetry surface"
            );
        }
    }
}

/// A sink that always refuses: an unavailable telemetry backend.
#[derive(Debug)]
struct UnavailableSink;

impl AgentDecisionEventSink for UnavailableSink {
    fn backend_name(&self) -> &'static str {
        "unavailable"
    }

    fn append<'a>(
        &'a self,
        _scope: &'a rakka_agent::AgentRunScope,
        _event: &'a rakka_agent::AgentDecisionEvent,
    ) -> AgentObservabilityFuture<'a, rakka_agent::AgentDecisionWriteStatus> {
        Box::pin(async {
            Err(AgentObservabilityError::Sink {
                code: "unavailable".to_string(),
                message: "the backend is down".to_string(),
            })
        })
    }

    fn read<'a>(
        &'a self,
        _scope: &'a rakka_agent::AgentRunScope,
        _after: u64,
        _limit: usize,
    ) -> AgentObservabilityFuture<'a, rakka_agent::AgentDecisionEventPage> {
        Box::pin(async {
            Err(AgentObservabilityError::Sink {
                code: "unavailable".to_string(),
                message: "the backend is down".to_string(),
            })
        })
    }
}

/// Scenario 26: an unavailable telemetry path blocks nothing — the run
/// completes — and its loss is visible through the bounded flush-failure
/// counter and the authoritative snapshot's owed count.
#[tokio::test]
async fn an_unavailable_sink_blocks_nothing_and_its_loss_is_visible() {
    let metrics = Arc::new(InMemoryMetricsRecorder::new());
    let fx = Fixture::new(full_run_dispatcher())
        .with_decision_events(Arc::new(UnavailableSink))
        .with_metrics(metrics.clone());
    fx.instantiate_agent().await;
    fx.create_task().await;
    fx.pump()
        .await
        .expect("correctness never waits on telemetry");

    let snapshot = fx.run_snapshot().await.expect("the run exists");
    assert_eq!(snapshot.status, AgentRunStatus::Completed);

    let failures = metrics
        .snapshot()
        .observations_named(METRIC_AGENT_TELEMETRY_FLUSH_FAILURES)
        .len();
    assert!(failures > 0, "the loss is visible as a bounded counter");

    let operational = agent_operational_snapshot(
        &fx.runs,
        &run_scope(),
        &AgentSchemaPolicy::default(),
        AgentTimestampMillis::new(9_999),
    )
    .await
    .expect("the point query answers")
    .expect("the run exists");
    assert!(
        operational.decisions_owed > 0,
        "the snapshot reports what the sink has not accepted"
    );
}

/// A fixture whose tool is checkpoint-required (shared with
/// `operational_query.rs`'s shape).
fn checkpointed_fixture() -> Fixture {
    use rakka_agent::{AgentEffectPolicies, AgentEffectSpec, InMemoryAgentRunEffectSink};
    use std::sync::atomic::AtomicU64;

    let policies = AgentEffectPolicies::new()
        .with_tool_spec(
            AgentToolId::new("charge-card").expect("tool id"),
            AgentEffectSpec::non_idempotent().with_checkpoint_required(),
        )
        .expect("the checkpoint-required tool spec is valid");
    Fixture::with_sink(
        ScriptedDispatcher::new()
            .with_turn(tool_calling_turn("charge-card", "SENSITIVE-ARG"))
            .with_turn(proposing_turn("SENSITIVE-ANSWER")),
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(AtomicU64::new(1)),
    )
}

/// Scenarios 23, 24, and 25 under the owner-kill sweep: kill the traced run's
/// owner at every durable write of the full flow, on both sides of the
/// compare-and-set. However the owner died, the converged run's durable facts
/// are identical to the untraced crash-free reference (the persisted chain
/// changed no effect behavior, and recovery kept it that way), the committing
/// segment still carries the ingress chain, and no telemetry surface —
/// decision events, metric observations, snapshot, session view — carries a
/// content or credential sentinel at the converged state. The run store is
/// the only store this flow's crash windows live in; the driver is the
/// in-process dispatcher, so owner kill at every write is the complete
/// boundary set here.
#[tokio::test]
async fn the_traced_flow_survives_any_owner_loss_without_changing_behavior() {
    // The untraced crash-free reference fixes the durable facts a traced,
    // crashed, recovered run must still converge on.
    let untraced = Fixture::new(full_run_dispatcher());
    untraced.instantiate_agent().await;
    untraced.runs.reset_writes();
    untraced.create_task().await;
    untraced.pump().await.expect("the untraced run completes");
    let writes = untraced.runs.writes();
    assert!(
        writes >= 6,
        "the full flow should make several durable writes, saw {writes}"
    );
    let reference = untraced.run_snapshot().await.expect("the run exists");

    rakka_agent::testkit::sweep_crash_points(writes, |nth, point| {
        let reference = reference.clone();
        async move {
            let sink = Arc::new(InMemoryAgentDecisionEventSink::new());
            let metrics = Arc::new(InMemoryMetricsRecorder::new());
            let fx = Fixture::new(full_run_dispatcher())
                .with_decision_events(sink.clone())
                .with_metrics(metrics.clone());
            fx.instantiate_agent().await;

            fx.runs.crash_at(nth, point);
            fx.create_task_traced(ingress_context(INGRESS_PARENT)).await;
            let _crashed = fx.pump().await;

            // A new owner activates and finds only what was durably committed.
            fx.runs.assert_crash_fired(nth, point);
            fx.runs.survive();
            fx.pump().await.unwrap_or_else(|error| {
                panic!("crash {point:?} at write {nth} did not converge: {error}")
            });

            // Scenario 23/24: same durable outcome as the untraced reference.
            let run = fx.run_snapshot().await.expect("the run exists");
            assert_eq!(
                (run.status, run.turn, run.budget),
                (reference.status, reference.turn, reference.budget),
                "crash {point:?} at write {nth}: tracing or recovery changed effect behavior"
            );

            // Scenario 23: the committing segment still carries the chain.
            let scope = run_scope();
            let state = load_agent_run_state(&fx.runs, &scope, &AgentSchemaPolicy::default())
                .await
                .expect("the run state loads")
                .expect("the run exists");
            let loop_state = state.loop_state().expect("the loop exists");
            assert_eq!(
                loop_state.telemetry().trace_parent.as_deref(),
                Some(INGRESS_PARENT),
                "crash {point:?} at write {nth} lost the ingress chain"
            );
            for effect in loop_state.effects() {
                assert_eq!(
                    effect.telemetry.trace_parent.as_deref(),
                    Some(INGRESS_PARENT),
                    "crash {point:?} at write {nth}: a retained effect lost its segment"
                );
            }

            // Scenario 25: the converged telemetry surfaces stay content-free.
            let mut surfaces = Vec::new();
            surfaces.push(
                serde_json::to_string(&sink.events(&scope)).expect("the decision events serialize"),
            );
            surfaces.push(format!("{:?}", metrics.snapshot().observations()));
            let snapshot = agent_operational_snapshot(
                &fx.runs,
                &scope,
                &AgentSchemaPolicy::default(),
                AgentTimestampMillis::new(9_999),
            )
            .await
            .expect("the point query answers")
            .expect("the run exists");
            surfaces.push(serde_json::to_string(&snapshot).expect("the snapshot serializes"));
            let view = assemble_agent_session_view(
                &fx.runs,
                &scope,
                &AgentSchemaPolicy::default(),
                Some(sink.as_ref()),
                AgentTimestampMillis::new(9_999),
            )
            .await
            .expect("the view assembles")
            .expect("the run exists");
            surfaces.push(serde_json::to_string(&view).expect("the view serializes"));
            for surface in &surfaces {
                for sentinel in [
                    "SENSITIVE-REASONING",
                    "SENSITIVE-ARG",
                    "SENSITIVE-RESULT",
                    "SENSITIVE-ANSWER",
                    "SECRET-TOKEN",
                ] {
                    assert!(
                        !surface.contains(sentinel),
                        "crash {point:?} at write {nth}: {sentinel} leaked into telemetry"
                    );
                }
            }
        }
    })
    .await;
}
