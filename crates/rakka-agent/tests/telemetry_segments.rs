//! Bounded operation segments, closed on the path a run actually takes.
//!
//! Specification: [17.4](../../../docs/plans/rakka-agent/spec.md) (bounded
//! trace segments), [17.6](../../../docs/plans/rakka-agent/spec.md) (the
//! required span model), [17.20](../../../docs/plans/rakka-agent/spec.md)
//! (the agent domain keeps its own stable vocabulary and puts the
//! OpenTelemetry mapping behind the `otel` feature).
//!
//! This suite exists because the mapping it feeds had none. The `otel` module
//! shipped fully unit-tested and entirely unreachable — nothing in the
//! workspace constructed one of its operations outside its own test block —
//! and the corrective is a call site on the path a run actually takes, not
//! more unit tests. So these assertions are about *emission*: which
//! operations a real run closes, in what order, carrying which identity and
//! which trace context. The convention mapping is asserted separately, over
//! the segments this produces.

use std::sync::Arc;

use rakka_agent::testkit::{DeterministicModelAdapter, ScriptedDispatcher};
use rakka_agent::{
    claim_append_operation_id, evaluation_operation_id, load_agent_run_state,
    promotion_operation_id, AgentApprovalDecision, AgentCheckpoint, AgentCheckpointDecision,
    AgentCheckpointKind, AgentClaimAppendRequest, AgentClaimObjectRequest, AgentEffectPolicies,
    AgentEffectResolution, AgentEffectSpec, AgentMemoryPromotionRequest, AgentModelTurn,
    AgentOperationId, AgentOperationKind, AgentPrivateMemoryKind, AgentReconciliationDecision,
    AgentRevisionNumber, AgentRunEffect, AgentRunEffectKind, AgentRunEffectOutcome,
    AgentRunEffectRequest, AgentRunEffectStatus, AgentRunEntityCommand, AgentRunEntityReply,
    AgentRunMemory, AgentRunStatus, AgentSchemaPolicy, AgentSegmentOperation, AgentSegmentOutcome,
    AgentSegmentSink, AgentTaskContent, AgentTaskEntityCommand, AgentTelemetrySegment,
    AgentToolCallId, AgentToolCallRequest, AgentToolId, AgentToolRegistry,
    InMemoryAgentRunEffectSink, InMemoryAgentSegmentSink, InMemoryContextSnapshotStore,
    InMemorySessionMemoryStore, KnowledgeSpaceId, MemoryClassification, MemorySequence,
    CURRENT_AGENT_LOOP_ADAPTER_VERSION, SEGMENT_ATTR_CHECKPOINT_KIND,
};
use rakka_agent_workflow::{AgentTelemetryContext, AgentTimestampMillis, PrincipalRef};

mod common;

use common::*;

const INGRESS_PARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

fn ingress_context() -> AgentTelemetryContext {
    AgentTelemetryContext {
        trace_parent: Some(INGRESS_PARENT.to_string()),
        ..AgentTelemetryContext::default()
    }
}

fn tool_calling_turn(tool: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Let me look that up.")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("call id"),
                AgentToolId::new(tool).expect("tool id"),
                serde_json::json!({ "query": "ticket" }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn(answer: &str) -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("I have an answer.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": answer }))
                .expect("the proposal is inline-bounded"),
        )
}

fn full_run_dispatcher() -> ScriptedDispatcher {
    ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(1, tool_calling_turn("lookup"))
            .with_turn_for(2, proposing_turn("resolved")),
    )
    .with_tool_result(
        "lookup",
        AgentTaskContent::inline(serde_json::json!({ "found": true }))
            .expect("the tool result is inline-bounded"),
    )
}

/// A run closes a segment for every loop transition it commits and one for
/// each resident execution slice, and each carries the run's identity and the
/// trace context the ingress command supplied.
#[tokio::test]
async fn a_run_closes_a_segment_for_every_committed_transition() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = Fixture::new(full_run_dispatcher()).with_segments(sink.clone());
    fx.instantiate_agent().await;
    fx.create_task_traced(ingress_context()).await;
    fx.pump().await.expect("the loop should run to completion");

    let segments = sink.segments();
    assert!(!segments.is_empty(), "a wired run closes segments");

    let decides = segments
        .iter()
        .filter(|segment| matches!(segment.operation, AgentSegmentOperation::Decide { .. }))
        .count();
    let invocations = segments
        .iter()
        .filter(|segment| matches!(segment.operation, AgentSegmentOperation::InvokeAgent { .. }))
        .count();
    assert!(decides > 0, "committed transitions close decide segments");
    // A resume is a durable wait being discharged — here, the model result
    // arriving at a run parked in `AwaitingModel`. It is closed only when the
    // wait actually ended, so a duplicate or a refusal closes none.
    assert!(
        segments
            .iter()
            .any(|segment| matches!(segment.operation, AgentSegmentOperation::RunResume)),
        "discharging a durable wait closes a resume segment: {:?}",
        sink.operations()
    );
    // The schedule row ends after durable acceptance into the outbox, not
    // after the transition that marked the effect ready — `Ready` does not
    // prove the sink write landed.
    assert!(
        segments.iter().any(|segment| matches!(
            segment.operation,
            AgentSegmentOperation::EffectSchedule { .. }
        )),
        "handing an effect to the outbox closes a schedule segment: {:?}",
        sink.operations()
    );
    assert!(
        invocations > 0,
        "each resident execution slice closes one invocation segment"
    );
    assert!(
        decides > invocations,
        "a resident slice advances more than one transition, so decides outnumber invocations"
    );

    // A segment is closed only after its transition committed, so every one
    // of them describes work that actually happened.
    assert!(segments
        .iter()
        .all(|segment| segment.outcome == AgentSegmentOutcome::Ok));

    // Identity rides the segment as an access-controlled attribute; the run
    // is the durable session identity of specification 17.3.
    let scope = run_scope();
    for segment in &segments {
        assert_eq!(
            segment.identity.run.as_deref(),
            Some(scope.run().as_str()),
            "every segment names the run it belongs to"
        );
        assert_eq!(
            segment.identity.agent.as_deref(),
            Some(scope.agent().as_str())
        );
    }

    // The persisted context is what links a segment to the operation that
    // caused it, across every passivation in between.
    assert!(
        segments
            .iter()
            .any(|segment| segment.telemetry.trace_parent.as_deref() == Some(INGRESS_PARENT)),
        "the ingress trace context reaches the segments the run closes"
    );

    // The invocation segment reports how much it advanced, as a bounded
    // number rather than as any identifier.
    let invocation = segments
        .iter()
        .find(|segment| matches!(segment.operation, AgentSegmentOperation::InvokeAgent { .. }))
        .expect("an invocation segment was closed");
    assert!(invocation
        .attributes
        .contains_key("rakka.agent.loop.transitions"));
}

/// Every segment names a bounded operation class and embeds no identifier in
/// it: the identity is an attribute, never part of the operation's name
/// (specification 17.6).
#[tokio::test]
async fn a_segments_operation_class_is_bounded_and_carries_no_identifier() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = Fixture::new(full_run_dispatcher()).with_segments(sink.clone());
    fx.instantiate_agent().await;
    fx.create_task_traced(ingress_context()).await;
    fx.pump().await.expect("the loop should run to completion");

    let scope = run_scope();
    for segment in sink.segments() {
        let label = segment.operation.as_label();
        assert!(
            !label.is_empty() && label.chars().all(|c| c.is_ascii_lowercase() || c == '-'),
            "{label} is not a stable kebab-case class"
        );
        // The agent name is deliberately absent: an `AgentId` is an
        // identifier however bounded it is, and 17.6 forbids one in a span
        // name. It rides the identity instead.
        if let AgentSegmentOperation::InvokeAgent { agent_name } = &segment.operation {
            assert!(
                agent_name.is_none(),
                "the run entity must not name the agent in the operation"
            );
        }
        let rendered = format!("{:?}", segment.operation);
        assert!(
            !rendered.contains(scope.run().as_str()),
            "an identifier reached the operation class: {rendered}"
        );
    }
}

/// An unwired run closes nothing and behaves identically: telemetry is never
/// a correctness input.
#[tokio::test]
async fn an_unwired_run_closes_no_segments() {
    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let fx = Fixture::new(full_run_dispatcher());
    fx.instantiate_agent().await;
    fx.create_task_traced(ingress_context()).await;
    fx.pump().await.expect("the loop should run to completion");

    assert!(
        sink.segments().is_empty(),
        "a sink the run was never wired with receives nothing"
    );
    assert_eq!(
        fx.run_snapshot().await.map(|snapshot| snapshot.status),
        Some(rakka_agent::AgentRunStatus::Completed),
        "the unwired run reaches the same terminal state"
    );
}

/// The dispatcher closes its own segments, and they come from the real
/// pipeline rather than from the scripted driver.
///
/// This distinction is the reason the test exists.
/// `ScriptedDispatcher::drive` bypasses `dispatch.rs` entirely — it awaits the
/// model adapter and applies `RecordEffectResult` directly — so the model,
/// tool, authorize, and dispatch rows are invisible to every `Fixture`-based
/// test however green it is. Instrumenting the scripted driver would have made
/// them visible and proved the testkit; driving the real `AgentRunEffectDispatcher`
/// proves the product.
#[tokio::test]
async fn the_real_dispatch_pipeline_closes_its_own_segments() {
    const TOOL: &str = "charge-card";

    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let registry = AgentToolRegistry::new()
        .register(tool_binding_for_spec(
            TOOL,
            &AgentEffectSpec::non_idempotent(),
        ))
        .expect("the tool registers");
    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn(TOOL))
        .with_turn_for(2, proposing_turn("charged"));
    let fx = AuthorityFixture::over(adapter, registry, None);
    fx.start().await;

    let mut pipeline = fx.pipeline().with_segments(sink.clone());
    for _round in 0..16 {
        fx.settle().await;
        let pass = pipeline
            .pump_run(&run_scope())
            .await
            .expect("the dispatch pass runs");
        let terminal = fx
            .fx
            .run_snapshot()
            .await
            .is_some_and(|run| run.status.is_terminal());
        if terminal {
            break;
        }
        if pass.registered == 0 && pass.claimed == 0 && pass.delivered == 0 {
            break;
        }
    }

    // The attributes specification 17.16's retention classes select on ride
    // the attempt that produced them, so a tail-sampling policy has something
    // to match rather than a rule that silently keeps nothing.
    let dispatch = sink
        .segments()
        .into_iter()
        .find(|segment| {
            matches!(
                segment.operation,
                AgentSegmentOperation::EffectDispatch { .. }
            )
        })
        .expect("a dispatch segment was closed");
    assert_eq!(
        dispatch
            .attributes
            .get(rakka_agent::SEGMENT_ATTR_EFFECT_STATUS)
            .map(String::as_str),
        Some("succeeded"),
        "the resolved effect status must be selectable"
    );
    assert!(dispatch
        .attributes
        .contains_key(rakka_agent::SEGMENT_ATTR_EFFECT_ATTEMPT));
    assert!(dispatch
        .attributes
        .contains_key(rakka_agent::SEGMENT_ATTR_SETTINGS_REVISION));

    let operations = sink.operations();
    for expected in ["tool-authorize", "effect-dispatch", "model-inference"] {
        assert!(
            operations.contains(&expected),
            "the dispatcher closed no `{expected}` segment; it closed {operations:?}"
        );
    }
    assert!(
        operations.contains(&"execute-tool"),
        "the tool row comes from the real executor, not the scripted driver: {operations:?}"
    );

    // Authorization ends before the durable `Started` write, so it is a
    // distinct interval from the attempt that follows it — and the attempt
    // never precedes its own grant.
    let authorize = operations
        .iter()
        .position(|operation| *operation == "tool-authorize")
        .expect("an authorize segment was closed");
    let dispatch = operations
        .iter()
        .position(|operation| *operation == "effect-dispatch")
        .expect("a dispatch segment was closed");
    assert!(authorize < dispatch);

    for segment in sink.segments() {
        assert_eq!(segment.outcome, AgentSegmentOutcome::Ok);
        assert!(segment.telemetry.trace_parent.is_some() || segment.identity.run.is_some());
    }
}

/// A checkpoint park closes its segment after the durable park, and names the
/// bounded checkpoint kind a retention policy selects escalation and timeout
/// on.
///
/// Specification 17.11 requires the opening span to end once the wait and its
/// notification are accepted, and forbids holding a span object across the
/// wait — so this asserts the segment exists *and* that the run is parked when
/// it does, which is the pair that makes the claim meaningful.
#[tokio::test]
async fn a_checkpoint_park_closes_its_segment_and_holds_none_open() {
    const TOOL: &str = "charge-card";

    let sink = Arc::new(InMemoryAgentSegmentSink::new());
    let registry = AgentToolRegistry::new()
        .register(
            tool_binding_for_spec(TOOL, &AgentEffectSpec::non_idempotent())
                .with_checkpoint_required(),
        )
        .expect("the tool registers");
    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn(TOOL))
        .with_turn_for(2, proposing_turn("charged"));
    // The checkpoint park is a *run entity* transition, so the sink has to
    // reach the entity as well as the dispatch pipeline. They are separate
    // wirings because in a deployment they are separate processes.
    let fx = AuthorityFixture::over(adapter, registry, None).with_segments(sink.clone());
    fx.start().await;

    let mut pipeline = fx.pipeline().with_segments(sink.clone());
    for _round in 0..16 {
        fx.settle().await;
        let pass = pipeline
            .pump_run(&run_scope())
            .await
            .expect("the dispatch pass runs");
        let parked = fx
            .fx
            .run_snapshot()
            .await
            .is_some_and(|run| run.status == rakka_agent::AgentRunStatus::WaitingForApproval);
        if parked {
            break;
        }
        if pass.registered == 0 && pass.claimed == 0 && pass.delivered == 0 {
            break;
        }
    }

    let run = fx.fx.run_snapshot().await.expect("the run exists");
    assert_eq!(
        run.status,
        rakka_agent::AgentRunStatus::WaitingForApproval,
        "the fixture must reach the checkpoint wait, or this proves nothing"
    );

    let checkpoints: Vec<_> = sink
        .segments()
        .into_iter()
        .filter(|segment| matches!(segment.operation, AgentSegmentOperation::CheckpointOpen))
        .collect();
    assert_eq!(
        checkpoints.len(),
        1,
        "the park closes exactly one checkpoint segment: {:?}",
        sink.operations()
    );
    assert!(checkpoints[0]
        .attributes
        .contains_key(rakka_agent::SEGMENT_ATTR_CHECKPOINT_KIND));
    // Ended, not held: the segment has both endpoints while the run is still
    // parked, which is what "no span object is held during passive wait"
    // means in a system that never holds one at all.
    assert!(checkpoints[0].duration_ms().is_some());
    assert_eq!(checkpoints[0].outcome, AgentSegmentOutcome::Ok);
}

// ---------------------------------------------------------------------------
// A gate a command opens exports its checkpoint-open segment (issue #126).
// ---------------------------------------------------------------------------

/// The knowledge space the fixture goal delegates, which a claim append names.
const SPACE: &str = "space-alpha";

/// The three run commands that record an effect and open its gate in the
/// command's own transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Commanded {
    /// `PromoteMemory`.
    Promotion,
    /// `AppendClaim`.
    ClaimAppend,
    /// `EvaluateGoal`.
    Evaluation,
}

impl Commanded {
    /// The commands a run accepts, in the order the tests apply them. An
    /// ended run evaluates no goal, but still promotes memory and appends
    /// claims inside its post-terminal window.
    fn accepted(ended: bool) -> &'static [Self] {
        if ended {
            &[Self::Promotion, Self::ClaimAppend]
        } else {
            &[Self::Promotion, Self::ClaimAppend, Self::Evaluation]
        }
    }

    /// The kind of effect the command records.
    fn effect_kind(self) -> AgentRunEffectKind {
        match self {
            Self::Promotion => AgentRunEffectKind::MemoryPromotionCall,
            Self::ClaimAppend => AgentRunEffectKind::ClaimAppendCall,
            Self::Evaluation => AgentRunEffectKind::GoalEvaluationCall,
        }
    }

    /// The command, under an operation id derived from `discriminator`.
    fn command(self, discriminator: &str) -> AgentRunEntityCommand {
        let scope = run_scope();
        match self {
            Self::Promotion => AgentRunEntityCommand::PromoteMemory {
                operation_id: promotion_operation_id(&scope, discriminator)
                    .expect("the operation id derives"),
                promotion: Box::new(promotion_request()),
            },
            Self::ClaimAppend => AgentRunEntityCommand::AppendClaim {
                operation_id: claim_append_operation_id(&scope, discriminator)
                    .expect("the operation id derives"),
                append: Box::new(claim_append_request()),
            },
            Self::Evaluation => AgentRunEntityCommand::EvaluateGoal {
                operation_id: evaluation_operation_id(&scope, discriminator)
                    .expect("the operation id derives"),
                evaluation: Box::new(goal_evaluation_request(AgentRevisionNumber::INITIAL)),
            },
        }
    }
}

/// The service principal that requests a promotion or a claim append.
fn curator() -> PrincipalRef {
    PrincipalRef {
        principal_type: "service".to_string(),
        principal_id: "memory-curator".to_string(),
        display_name: None,
    }
}

/// A promotion of the task input, the first entry the session holds.
fn promotion_request() -> AgentMemoryPromotionRequest {
    AgentMemoryPromotionRequest {
        from_sequence: MemorySequence::new(1),
        to_sequence: MemorySequence::new(1),
        kind: AgentPrivateMemoryKind::Semantic,
        target: None,
        confidence_bps: 9_000,
        requested_by: curator(),
        roles: None,
    }
}

/// A claim append into [`SPACE`].
fn claim_append_request() -> AgentClaimAppendRequest {
    AgentClaimAppendRequest {
        space: KnowledgeSpaceId::new(SPACE).expect("the space id is valid"),
        subject: "finding".to_string(),
        predicate: "links".to_string(),
        object: AgentClaimObjectRequest::Value(
            AgentTaskContent::inline(serde_json::json!({ "note": "observed" }))
                .expect("the object is inline-bounded"),
        ),
        confidence_bps: 5_000,
        classification: MemoryClassification::Unclassified,
        evidence: Vec::new(),
        requested_by: curator(),
    }
}

/// Which gate a deployment's specs require.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    /// A checkpoint: the gate is an `Approval`.
    Checkpoint,
    /// An authorization: the gate is a `SecurityAuthorization`.
    Authorization,
}

impl Gate {
    const ALL: [Self; 2] = [Self::Checkpoint, Self::Authorization];

    /// `spec`, requiring this gate.
    fn require(self, spec: AgentEffectSpec) -> AgentEffectSpec {
        match self {
            Self::Checkpoint => spec.with_checkpoint_required(),
            Self::Authorization => spec.with_authorization_required(),
        }
    }

    /// The checkpoint kind the gate opens as.
    fn kind(self) -> AgentCheckpointKind {
        match self {
            Self::Checkpoint => AgentCheckpointKind::Approval,
            Self::Authorization => AgentCheckpointKind::SecurityAuthorization,
        }
    }
}

/// Policies under which a promotion, a claim append and a goal evaluation
/// each require `gate`. The evaluation keeps its default spec otherwise.
fn policies_gating(gate: Gate) -> AgentEffectPolicies {
    let policies = AgentEffectPolicies::new();
    let evaluation = policies
        .spec_for(&AgentRunEffectRequest::Evaluation {
            evaluation: Box::new(goal_evaluation_request(AgentRevisionNumber::INITIAL)),
        })
        .clone();
    let idempotent = || AgentEffectSpec::idempotent(3).expect("the spec is valid");
    policies
        .with_memory_promotion_spec(gate.require(idempotent()))
        .and_then(|policies| policies.with_claim_append_spec(gate.require(idempotent())))
        .and_then(|policies| policies.with_goal_evaluation_spec(gate.require(evaluation)))
        .expect("the gated policies are valid")
}

/// A traced goal-bearing run under `policies`, on which all three commands
/// can commit: its goal names an evaluator and delegates [`SPACE`], and its
/// session memory is wired. Live, it awaits its first model call; `ended`, it
/// has run to `Completed`. No segment sink is wired, so a sink the test wires
/// afterwards holds only what its own commands export.
async fn commandable_world(policies: AgentEffectPolicies, ended: bool) -> Fixture {
    let dispatcher = ScriptedDispatcher::with_adapter(
        DeterministicModelAdapter::new()
            .with_turn_for(
                1,
                AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION).with_text("thinking"),
            )
            .with_turn_for(2, proposing_turn("resolved")),
    );
    let fx = Fixture::with_sink(
        dispatcher,
        InMemoryAgentRunEffectSink::new(),
        policies,
        Arc::new(std::sync::atomic::AtomicU64::new(1)),
    )
    .with_memory(AgentRunMemory::new(
        Arc::new(InMemorySessionMemoryStore::new()),
        Arc::new(InMemoryContextSnapshotStore::new()),
    ));
    fx.instantiate_agent().await;
    let mut spec = goal_spec_with_evaluator();
    spec.knowledge_spaces
        .insert(KnowledgeSpaceId::new(SPACE).expect("the space id is valid"));
    let mut creation = goal_task_creation_command(task_definition(), goal_spec_draft(spec, true));
    // The effects a command records carry the run's trace context, and a
    // parked identity derives only from a traced one.
    if let AgentTaskEntityCommand::Create { creation, .. } = &mut creation {
        creation.telemetry = ingress_context();
    }
    fx.apply_task_command(creation)
        .await
        .expect("the goal task creates");
    if ended {
        fx.pump().await.expect("the loop runs to its end");
        let run = fx.run_snapshot().await.expect("the run exists");
        assert_eq!(run.status, AgentRunStatus::Completed, "the run ended");
    } else {
        let mut run = fx.run();
        let now = fx.now();
        run.recover(now).await.expect("the run recovers");
        run.settle_side_effects(&fx.router, now)
            .await
            .expect("the run settles");
        let run = fx.run_snapshot().await.expect("the run exists");
        assert!(!run.status.is_terminal(), "the run is live");
    }
    fx
}

/// Applies `command` to the run through a freshly recovered entity.
async fn apply_run(fx: &Fixture, command: AgentRunEntityCommand) -> AgentRunEntityReply {
    let mut run = fx.run();
    let now = fx.now();
    run.recover(now).await.expect("the run recovers");
    run.apply(command, &fx.router, now)
        .await
        .expect("the command applies")
}

/// The run's one effect of `kind`, and the checkpoints open on it.
async fn effect_and_gates(
    fx: &Fixture,
    kind: AgentRunEffectKind,
) -> (AgentRunEffect, Vec<AgentCheckpoint>) {
    let state = load_agent_run_state(&fx.runs, &run_scope(), &AgentSchemaPolicy::default())
        .await
        .expect("the run state loads")
        .expect("the run exists");
    let loop_state = state.loop_state().expect("the loop is started");
    let effects: Vec<&AgentRunEffect> = loop_state
        .effects()
        .iter()
        .filter(|effect| effect.kind() == kind)
        .collect();
    assert_eq!(effects.len(), 1, "the run holds one {kind} effect");
    let effect = effects[0].clone();
    let gates = loop_state
        .open_checkpoints()
        .iter()
        .filter(|checkpoint| checkpoint.bound_effect.effect_id == effect.effect_id)
        .cloned()
        .collect();
    (effect, gates)
}

/// Every `checkpoint-open` segment the sink holds, in the order they closed.
fn checkpoint_opens(sink: &InMemoryAgentSegmentSink) -> Vec<AgentTelemetrySegment> {
    sink.segments()
        .into_iter()
        .filter(|segment| matches!(segment.operation, AgentSegmentOperation::CheckpointOpen))
        .collect()
}

/// Issue #126: a gate a command opens in its own transition exports its
/// `checkpoint-open` segment, as a gate the loop opens does.
///
/// `PromoteMemory`, `AppendClaim` and `EvaluateGoal` are applied in turn to
/// one run, under specs that gate all three, on a live run and (for the two
/// kinds an ended run still accepts) on an ended one. Each command exports
/// exactly one segment: the second and third are applied with the earlier
/// gates still open, and export none of those again. The segment names the
/// gate's kind and exports under the parked identity the checkpoint stores,
/// so the gate's later resolution links a span that was exported
/// ([specification 17.11]). Without the export, none of the commands
/// exported anything.
#[tokio::test]
async fn a_commanded_gate_exports_its_checkpoint_open_segment() {
    let mut exported = Vec::new();
    let mut opened = Vec::new();
    for gate in Gate::ALL {
        for ended in [false, true] {
            let sink = Arc::new(InMemoryAgentSegmentSink::new());
            let fx = commandable_world(policies_gating(gate), ended)
                .await
                .with_segments(sink.clone());
            for &commanded in Commanded::accepted(ended) {
                let at = format!("{gate:?}, ended: {ended}, {commanded:?}");
                let before = checkpoint_opens(&sink).len();
                let reply = apply_run(&fx, commanded.command("gated")).await;
                assert!(
                    matches!(reply, AgentRunEntityReply::Applied { .. }),
                    "{at}: {reply:?}"
                );
                let (effect, gates) = effect_and_gates(&fx, commanded.effect_kind()).await;
                assert_eq!(gates.len(), 1, "{at}: the command opened its gate");
                assert_eq!(gates[0].kind, gate.kind(), "{at}");
                let segments = checkpoint_opens(&sink).split_off(before);
                exported.push((at.clone(), segments.len()));
                opened.push((at, effect, gates[0].clone(), segments));
            }
        }
    }
    let expected: Vec<_> = exported.iter().map(|(at, _)| (at.clone(), 1)).collect();
    assert_eq!(
        exported, expected,
        "each command exports exactly one checkpoint-open segment"
    );

    for (at, effect, gate, segments) in opened {
        let segment = &segments[0];
        assert_eq!(
            segment
                .attributes
                .get(SEGMENT_ATTR_CHECKPOINT_KIND)
                .map(String::as_str),
            Some(gate.kind.as_label()),
            "{at}: the segment names the gate's kind"
        );
        let parked = AgentCheckpoint::parked_span_identity(&effect.telemetry, &gate.checkpoint_id)
            .unwrap_or_else(|| panic!("{at}: a traced effect derives a parked identity"));
        assert_eq!(
            segment.span_id.as_deref(),
            Some(parked.span_id.as_str()),
            "{at}: the segment exports under the parked identity"
        );
        assert_eq!(
            gate.telemetry.trace_parent.as_deref(),
            Some(parked.trace_parent().as_str()),
            "{at}: the checkpoint stores the identity the segment exports under"
        );
        assert_eq!(segment.outcome, AgentSegmentOutcome::Ok, "{at}");
        assert!(segment.duration_ms().is_some(), "{at}: the segment ended");
    }
}

/// The same three commands under specs that gate nothing open no gate and
/// export no `checkpoint-open` segment, on a live run and on an ended one.
#[tokio::test]
async fn an_ungated_command_exports_no_checkpoint_open_segment() {
    for ended in [false, true] {
        let sink = Arc::new(InMemoryAgentSegmentSink::new());
        let fx = commandable_world(AgentEffectPolicies::default(), ended)
            .await
            .with_segments(sink.clone());
        for &commanded in Commanded::accepted(ended) {
            let at = format!("ended: {ended}, {commanded:?}");
            let reply = apply_run(&fx, commanded.command("ungated")).await;
            assert!(
                matches!(reply, AgentRunEntityReply::Applied { .. }),
                "{at}: {reply:?}"
            );
            let (effect, gates) = effect_and_gates(&fx, commanded.effect_kind()).await;
            assert!(
                !effect.checkpoint_required && !effect.authorization_required,
                "{at}: the spec gates nothing"
            );
            assert!(gates.is_empty(), "{at}: no gate opened: {gates:?}");
        }
        assert!(
            !sink.segments().is_empty(),
            "ended: {ended}: the sink is wired and the commands' settle passes closed segments"
        );
        assert!(
            checkpoint_opens(&sink).is_empty(),
            "ended: {ended}: no command exported a checkpoint-open segment: {:?}",
            sink.operations()
        );
    }
}

/// A replayed gated command answers `Duplicate` from the operation log,
/// opens nothing and exports no second `checkpoint-open` segment: each gate
/// is exported once, by the command that opened it.
#[tokio::test]
async fn a_replayed_gated_command_exports_no_second_segment() {
    for ended in [false, true] {
        let sink = Arc::new(InMemoryAgentSegmentSink::new());
        let fx = commandable_world(policies_gating(Gate::Checkpoint), ended)
            .await
            .with_segments(sink.clone());
        for &commanded in Commanded::accepted(ended) {
            let at = format!("ended: {ended}, {commanded:?}");
            let command = commanded.command("replayed");
            let reply = apply_run(&fx, command.clone()).await;
            assert!(
                matches!(reply, AgentRunEntityReply::Applied { .. }),
                "{at}: {reply:?}"
            );
            let exported = checkpoint_opens(&sink);
            let (_, gates) = effect_and_gates(&fx, commanded.effect_kind()).await;

            let replay = apply_run(&fx, command).await;
            assert!(
                matches!(replay, AgentRunEntityReply::Duplicate { .. }),
                "{at}: {replay:?}"
            );
            assert_eq!(
                checkpoint_opens(&sink),
                exported,
                "{at}: the replay exported no segment"
            );
            assert_eq!(
                effect_and_gates(&fx, commanded.effect_kind()).await.1,
                gates,
                "{at}: the replay opened nothing"
            );
        }
        assert_eq!(
            checkpoint_opens(&sink).len(),
            Commanded::accepted(ended).len(),
            "ended: {ended}: each gate was exported exactly once"
        );
    }
}

/// The two commands that answer a parked generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resolving {
    /// `ResolveIndeterminateEffect`.
    Effect,
    /// `ResolveCheckpoint`, on the reconciliation checkpoint.
    Checkpoint,
}

impl Resolving {
    const ALL: [Self; 2] = [Self::Effect, Self::Checkpoint];

    /// The command answering `parked`, whose reconciliation checkpoint is
    /// `reconciliation`: `ConfirmedExecuted` with `executed` when it is given,
    /// else `ConfirmedNotExecuted`.
    fn command(
        self,
        key: &str,
        parked: &AgentRunEffect,
        reconciliation: &AgentCheckpoint,
        executed: Option<AgentRunEffectOutcome>,
    ) -> AgentRunEntityCommand {
        let operation_id = AgentOperationId::for_agent(
            AgentOperationKind::CheckpointResolution,
            &agent_scope(),
            key,
        )
        .expect("the decision key derives");
        let resolution = match executed {
            Some(outcome) => AgentEffectResolution::ConfirmedExecuted {
                outcome: Box::new(outcome),
            },
            None => AgentEffectResolution::ConfirmedNotExecuted,
        };
        match self {
            Self::Effect => AgentRunEntityCommand::ResolveIndeterminateEffect {
                operation_id,
                effect_id: parked.effect_id.clone(),
                generation: parked.generation,
                resolution: Box::new(resolution),
                telemetry: AgentTelemetryContext::default(),
            },
            Self::Checkpoint => {
                let decision = if resolution == AgentEffectResolution::ConfirmedNotExecuted {
                    AgentReconciliationDecision::ConfirmedNotExecuted
                } else {
                    AgentReconciliationDecision::ConfirmedCompleted {
                        resolution: Box::new(resolution),
                    }
                };
                AgentRunEntityCommand::ResolveCheckpoint {
                    operation_id,
                    checkpoint_id: reconciliation.checkpoint_id.clone(),
                    resolver: curator(),
                    decision: Box::new(AgentCheckpointDecision::Reconciliation(decision)),
                    telemetry: AgentTelemetryContext::default(),
                }
            }
        }
    }
}

/// Commands a promotion on `fx`'s live run, grants its gate when the spec
/// opened one, and parks its dispatched generation `Indeterminate`. Returns
/// the parked effect and the reconciliation checkpoint the park opened.
async fn parked_promotion(fx: &Fixture) -> (AgentRunEffect, AgentCheckpoint) {
    let reply = apply_run(fx, Commanded::Promotion.command("parked")).await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    let (_, gates) = effect_and_gates(fx, AgentRunEffectKind::MemoryPromotionCall).await;
    if let Some(gate) = gates.first() {
        let grant = AgentRunEntityCommand::ResolveCheckpoint {
            operation_id: AgentOperationId::for_agent(
                AgentOperationKind::CheckpointResolution,
                &agent_scope(),
                "grant",
            )
            .expect("the decision key derives"),
            checkpoint_id: gate.checkpoint_id.clone(),
            resolver: curator(),
            decision: Box::new(AgentCheckpointDecision::Approval(
                AgentApprovalDecision::Approve {
                    credential_binding: None,
                    expires_at: AgentTimestampMillis::new(u64::MAX / 2),
                    allowed_use_count: 1,
                },
            )),
            telemetry: AgentTelemetryContext::default(),
        };
        let reply = apply_run(fx, grant).await;
        assert!(
            matches!(reply, AgentRunEntityReply::Applied { .. }),
            "{reply:?}"
        );
    }
    let (dispatched, _) = effect_and_gates(fx, AgentRunEffectKind::MemoryPromotionCall).await;
    assert_eq!(dispatched.status, AgentRunEffectStatus::Ready, "dispatched");
    let reply = apply_run(
        fx,
        AgentRunEntityCommand::RecordEffectResult {
            operation_id: dispatched
                .result_operation_id(&run_scope())
                .expect("the result operation id derives"),
            effect_id: dispatched.effect_id.clone(),
            generation: dispatched.generation,
            attempt: dispatched.attempts.saturating_add(1),
            fence: 0,
            outcome: Box::new(AgentRunEffectOutcome::indeterminate(
                "dispatcher-lost-after-started",
                "the recovery retry was refused",
            )),
        },
    )
    .await;
    assert!(
        matches!(reply, AgentRunEntityReply::Applied { .. }),
        "{reply:?}"
    );
    let (parked, gates) = effect_and_gates(fx, AgentRunEffectKind::MemoryPromotionCall).await;
    assert_eq!(parked.status, AgentRunEffectStatus::Indeterminate);
    assert_eq!(
        gates.len(),
        1,
        "the park opened its reconciliation checkpoint"
    );
    assert_eq!(
        gates[0].kind,
        AgentCheckpointKind::IndeterminateEffectReconciliation
    );
    (parked, gates[0].clone())
}

/// Issue #126: a gated generation re-sent by a person's `ConfirmedNotExecuted`
/// opens the new generation's gate in the resolution's own transition, and
/// that gate exports its `checkpoint-open` segment as every other gate does.
///
/// A gated promotion is granted, dispatched and parked `Indeterminate` on a
/// live run, then answered through `ResolveIndeterminateEffect` and, in a
/// second world, through `ResolveCheckpoint`. Each answer exports exactly one
/// segment: the new gate's, naming its kind and exporting under the parked
/// identity the new checkpoint stores. Without it the gate's later resolution
/// would link a parked span that was never exported
/// ([specification 17.11]). A resolution that opens no gate exports none: an
/// ungated promotion re-sent the same way, and a gated one confirmed
/// executed.
#[tokio::test]
async fn a_re_sent_gated_generation_exports_its_checkpoint_open_segment() {
    for gate in Gate::ALL {
        for resolving in Resolving::ALL {
            let at = format!("{gate:?}, {resolving:?}");
            let sink = Arc::new(InMemoryAgentSegmentSink::new());
            let fx = Box::pin(commandable_world(policies_gating(gate), false))
                .await
                .with_segments(sink.clone());
            let (parked, reconciliation) = parked_promotion(&fx).await;
            let before = checkpoint_opens(&sink).len();

            let reply = apply_run(
                &fx,
                resolving.command("not-executed", &parked, &reconciliation, None),
            )
            .await;
            assert!(
                matches!(reply, AgentRunEntityReply::Applied { .. }),
                "{at}: {reply:?}"
            );
            let (resent, gates) =
                effect_and_gates(&fx, AgentRunEffectKind::MemoryPromotionCall).await;
            assert_eq!(resent.generation, parked.generation.next(), "{at}");
            assert_eq!(gates.len(), 1, "{at}: the new generation's gate: {gates:?}");
            let new_gate = &gates[0];
            assert_eq!(new_gate.kind, gate.kind(), "{at}");
            assert_eq!(new_gate.bound_effect.generation, resent.generation, "{at}");

            let segments = checkpoint_opens(&sink).split_off(before);
            assert_eq!(
                segments.len(),
                1,
                "{at}: the resolution exports exactly one checkpoint-open segment: {:?}",
                sink.operations()
            );
            let segment = &segments[0];
            assert_eq!(
                segment
                    .attributes
                    .get(SEGMENT_ATTR_CHECKPOINT_KIND)
                    .map(String::as_str),
                Some(gate.kind().as_label()),
                "{at}: the segment names the gate's kind"
            );
            let identity =
                AgentCheckpoint::parked_span_identity(&resent.telemetry, &new_gate.checkpoint_id)
                    .unwrap_or_else(|| panic!("{at}: a traced effect derives a parked identity"));
            assert_eq!(
                segment.span_id.as_deref(),
                Some(identity.span_id.as_str()),
                "{at}: the segment exports under the parked identity"
            );
            assert_eq!(
                new_gate.telemetry.trace_parent.as_deref(),
                Some(identity.trace_parent().as_str()),
                "{at}: the checkpoint stores the identity the segment exports under"
            );
            assert_eq!(segment.outcome, AgentSegmentOutcome::Ok, "{at}");
            assert!(segment.duration_ms().is_some(), "{at}: the segment ended");
        }
    }

    // Resolutions that open no gate export no segment.
    for resolving in Resolving::ALL {
        for (label, policies, executed) in [
            (
                "ungated, not executed",
                AgentEffectPolicies::default(),
                None,
            ),
            (
                "gated, executed",
                policies_gating(Gate::Checkpoint),
                Some(AgentRunEffectOutcome::MemoryPromotion {
                    promoted: Vec::new(),
                }),
            ),
        ] {
            let at = format!("{label}, {resolving:?}");
            let sink = Arc::new(InMemoryAgentSegmentSink::new());
            let fx = Box::pin(commandable_world(policies, false))
                .await
                .with_segments(sink.clone());
            let (parked, reconciliation) = parked_promotion(&fx).await;
            let before = checkpoint_opens(&sink).len();

            let reply = apply_run(
                &fx,
                resolving.command("resolved", &parked, &reconciliation, executed),
            )
            .await;
            assert!(
                matches!(reply, AgentRunEntityReply::Applied { .. }),
                "{at}: {reply:?}"
            );
            let (_, gates) = effect_and_gates(&fx, AgentRunEffectKind::MemoryPromotionCall).await;
            assert!(gates.is_empty(), "{at}: no gate opened: {gates:?}");
            assert_eq!(
                checkpoint_opens(&sink).len(),
                before,
                "{at}: the resolution exported no checkpoint-open segment: {:?}",
                sink.operations()
            );
        }
    }
}

/// The in-memory sink is bounded, like the trait says every sink must be.
///
/// It pushed into an unbounded `Vec`, six lines below the paragraph stating
/// that [17.1] forbids unbounded in-process queues and that a sink which
/// cannot keep up must drop and count. That was not merely an inconsistent
/// test helper: the only other implementation in the workspace is behind the
/// `otel` feature, so under `--no-default-features` this was the only thing a
/// deployment could pass to `with_segments`, and nothing drained it.
#[test]
fn the_in_memory_sink_is_bounded_and_counts_what_it_drops() {
    let sink = InMemoryAgentSegmentSink::with_capacity(3);
    let phases = [
        "preparing-context",
        "deciding-continuation",
        "awaiting-effects",
        "recording-results",
        "deciding-completion",
    ];
    for (index, phase) in phases.iter().enumerate() {
        let at = rakka_agent_workflow::AgentTimestampMillis::new(index as u64 + 1);
        sink.record(
            &rakka_agent::AgentTelemetrySegment::new(
                AgentSegmentOperation::Decide { phase },
                at,
                at,
            )
            .telemetry(ingress_context())
            .ok(),
        );
    }

    let retained = sink.segments();
    assert_eq!(retained.len(), 3, "the bound holds");
    assert_eq!(sink.dropped(), 2, "and the loss is counted, not silent");
    // The oldest went, so the most recent operations — the ones an incident is
    // usually about — are the ones kept, as in the exporter's ring.
    assert_eq!(
        retained
            .iter()
            .map(|segment| segment.start.as_millis())
            .collect::<Vec<_>>(),
        vec![3, 4, 5]
    );

    // A capacity of zero would otherwise drop everything while reporting a
    // healthy sink.
    let degenerate = InMemoryAgentSegmentSink::with_capacity(0);
    degenerate.record(
        &rakka_agent::AgentTelemetrySegment::new(
            AgentSegmentOperation::Decide {
                phase: "preparing-context",
            },
            rakka_agent_workflow::AgentTimestampMillis::new(1),
            rakka_agent_workflow::AgentTimestampMillis::new(1),
        )
        .telemetry(ingress_context())
        .ok(),
    );
    assert_eq!(degenerate.segments().len(), 1);
    assert_eq!(degenerate.dropped(), 0);
}

/// A bounded sink's loss is published, and published as a **diff**.
///
/// [17.12](../../../docs/plans/rakka-agent/spec.md) asks for telemetry export
/// queue and drops, and before slice 6.3b the only recording site in the crate
/// was the durable decision sink's refusal — a segment sink dropped in total
/// silence. Both halves are asserted here because both can fail
/// independently: a sink that publishes nothing reports a healthy pipeline
/// while losing spans, and a sink that publishes its *cumulative* counter into
/// an adding counter reports the triangular sum of its loss instead of the
/// loss. The second is the reason `AgentSegmentSinkHealth` holds state at all.
#[test]
fn a_bounded_sink_publishes_its_loss_once_per_drop() {
    let metrics = Arc::new(rakka_core::InMemoryMetricsRecorder::new());
    let sink = InMemoryAgentSegmentSink::with_capacity(1).with_metrics(metrics.clone());

    let segment = |at: u64| {
        rakka_agent::AgentTelemetrySegment::new(
            AgentSegmentOperation::Decide { phase: "propose" },
            rakka_agent_workflow::AgentTimestampMillis::new(at),
            rakka_agent_workflow::AgentTimestampMillis::new(at + 1),
        )
        .ok()
    };

    // The first fills the ring; the next three each evict one.
    for at in 0..4 {
        sink.record(&segment(at));
    }
    assert_eq!(sink.dropped(), 3, "capacity 1 evicts three of four");

    let snapshot = metrics.snapshot();
    let queue = snapshot.observations_named(rakka_agent::METRIC_AGENT_TELEMETRY_EXPORT_QUEUE);
    assert!(
        !queue.is_empty(),
        "the queue depth is published, so a drop is not the first an operator hears of it"
    );
    let drops: f64 = snapshot
        .observations_named(rakka_agent::METRIC_AGENT_TELEMETRY_EXPORT_DROPS)
        .iter()
        .map(|observation| observation.value())
        .sum();
    assert!(
        (drops - 3.0).abs() < f64::EPSILON,
        "three evictions must total three, not the cumulative sum of a growing counter; saw {drops}"
    );

    // One more eviction adds exactly one, not another whole history.
    sink.record(&segment(9));
    let drops: f64 = metrics
        .snapshot()
        .observations_named(rakka_agent::METRIC_AGENT_TELEMETRY_EXPORT_DROPS)
        .iter()
        .map(|observation| observation.value())
        .sum();
    assert!(
        (drops - 4.0).abs() < f64::EPSILON,
        "a fourth eviction adds one; saw {drops}"
    );
}

/// A health watermark that can rewind reports more loss than the sink took.
///
/// `AgentSegmentSinkHealth` diffs a cumulative counter, and the watermark it
/// diffs against was advanced with `swap`. That is atomic on its own, but the
/// pair *(read the sink's counter, swap the watermark)* is not — and every
/// driver of a run shares one `Arc<dyn AgentSegmentSink>` across threads, so
/// two publishers can land out of order. The publisher that saw the higher
/// count reports it, then the one that saw the lower count rewinds the
/// watermark to a value already published, and the next publish counts that
/// interval a second time.
///
/// The interleaving is reproduced here directly rather than raced for: the
/// three calls below are exactly what those two threads plus the next publish
/// hand the type, in the order they would arrive. Under `swap` the total is
/// ten for eight real drops; under an advance-only watermark a late lower
/// observation owes nothing and moves nothing.
#[test]
fn a_health_watermark_never_rewinds() {
    let metrics = rakka_core::InMemoryMetricsRecorder::new();
    let health = rakka_agent::AgentSegmentSinkHealth::new();

    // The thread that observed 7 publishes first, then the one that observed
    // 5, then a later publish at 8.
    health.publish(&metrics, "in-memory", None, 7, 0);
    health.publish(&metrics, "in-memory", None, 5, 0);
    health.publish(&metrics, "in-memory", None, 8, 0);

    let drops: f64 = metrics
        .snapshot()
        .observations_named(rakka_agent::METRIC_AGENT_TELEMETRY_EXPORT_DROPS)
        .iter()
        .map(|observation| observation.value())
        .sum();
    assert!(
        (drops - 8.0).abs() < f64::EPSILON,
        "the published total is the highest count observed, not the sum of a \
         rewound interval counted twice; saw {drops}"
    );
}

/// The queue gauge traces the ring filling, then stops writing.
///
/// It was published only inside the drop branch, where `segments.len()` is
/// always exactly `capacity` — the eviction loop exits one below the bound and
/// the push restores it. So the "leading indicator" was a constant an operator
/// could read only at saturation, and once the ring was full it was rewritten
/// on the path of every loop transition, dispatch attempt, model call and A2A
/// request, for a number that could not change.
///
/// Both halves are asserted: the depths that reach the recorder are the fill
/// curve, and eight records produce four writes rather than eight.
#[test]
fn a_bounded_sink_publishes_a_queue_depth_that_moves() {
    let metrics = Arc::new(rakka_core::InMemoryMetricsRecorder::new());
    let sink = InMemoryAgentSegmentSink::with_capacity(4).with_metrics(metrics.clone());
    for at in 0..8 {
        sink.record(
            &rakka_agent::AgentTelemetrySegment::new(
                AgentSegmentOperation::Decide { phase: "propose" },
                rakka_agent_workflow::AgentTimestampMillis::new(at),
                rakka_agent_workflow::AgentTimestampMillis::new(at + 1),
            )
            .ok(),
        );
    }
    assert_eq!(sink.dropped(), 4, "capacity 4 evicts four of eight");

    let snapshot = metrics.snapshot();
    let depths: Vec<u64> = snapshot
        .observations_named(rakka_agent::METRIC_AGENT_TELEMETRY_EXPORT_QUEUE)
        .iter()
        .map(|observation| observation.value() as u64)
        .collect();
    assert_eq!(
        depths,
        vec![1, 2, 3, 4],
        "the gauge is the fill curve, written once per change and never again \
         once the ring saturates"
    );
}
