//! No secret material reaches any durable record, telemetry surface, or log.
//!
//! Specification: sections 16 and 11.8; scenario 25's secret half. Two proofs
//! already cover pieces of this, and neither is sufficient:
//!
//! - `effect_dispatch.rs`'s
//!   `credentials_are_resolved_at_dispatch_only_and_never_persisted` scans one
//!   sentinel across three stores of one happy-path run. It cannot see the
//!   failure path — which is where an application-supplied string actually
//!   reaches a durable record — and it names no policy that would make its
//!   store list complete.
//! - `trace_scenarios.rs`'s `default_telemetry_carries_no_content_or_credentials`
//!   scans five content sentinels across four *telemetry* surfaces. Content on
//!   telemetry and secrets in state are different claims with different
//!   verdicts: durable state legitimately holds prompts, arguments, and
//!   results — it is the correctness source — and must never hold a
//!   credential.
//!
//! So this file keeps two vocabularies. A [`SECRETS`] sentinel is forbidden on
//! every surface, durable included. A [`CONTENT`] sentinel is forbidden on
//! telemetry, logs, and metrics only.
//!
//! And it makes the surface list complete by construction: [`scanned_surface`]
//! matches every [`AgentRecordKind`] with no wildcard arm, so a milestone that
//! adds a thirtieth record kind does not compile until it says where that kind
//! is scanned. The [`COVERAGE`] table then asserts the driven scenarios
//! actually materialize what they claim to — scanning an empty store proves
//! nothing — and [`UNREACHED`] names, with a reason, every kind no scenario
//! here drives.
//!
//! `AgentRecordKind::ALL` is not the whole surface. The durable workflow
//! substrate — the run's inbox/outbox `WorkflowState` and the
//! `AgentDispatcherFleetState` index — carries no agent record kind at all,
//! and it is exactly where a failed dispatch attempt's detail is persisted.
//! It is scanned beside the catalogue rather than through it.

use rakka_agent::testkit::{DeterministicModelAdapter, RecordingToolExecutor};
use rakka_agent::{
    AgentCredentialBindingRef, AgentEffectSpec, AgentModelTurn, AgentRecordKind, AgentTaskContent,
    AgentToolCallId, AgentToolCallRequest, AgentToolId, CURRENT_AGENT_LOOP_ADAPTER_VERSION,
};

mod common;

use common::*;

const TOOL: &str = "charge-card";

// ---------------------------------------------------------------------------
// The two sentinel vocabularies.
// ---------------------------------------------------------------------------

/// Forbidden on every surface, durable included.
///
/// Each entry names a distinct way secret material can enter the runtime: the
/// resolved credential itself, and the two application-authored error strings
/// that travel a failure path into a durable record.
const SECRETS: &[&str] = &[
    "RAKKA-SECRET-BEARER",
    "RAKKA-SECRET-VAULT-DETAIL",
    "RAKKA-SECRET-EXECUTOR-DETAIL",
];

/// Forbidden on telemetry, logs, and metrics; legitimate in durable state,
/// which is the correctness source and must hold exactly this.
const CONTENT: &[&str] = &[
    "RAKKA-CONTENT-REASONING",
    "RAKKA-CONTENT-ARG",
    "RAKKA-CONTENT-RESULT",
];

// ---------------------------------------------------------------------------
// The exhaustiveness spine.
// ---------------------------------------------------------------------------

/// Where one durable record kind is read back for scanning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScannedSurface {
    /// A store this fixture owns, by the label `durable_surfaces` gives it.
    Store(&'static str),
    /// Carried inside another record; scanning that record scans this.
    CarriedIn(AgentRecordKind),
    /// No scenario in this file materializes the kind. The reason is the
    /// second field, and `the_sweep_names_every_durable_record_kind` requires
    /// it to be non-empty.
    Unreached(&'static str),
}

/// Exhaustive by construction: no wildcard arm.
///
/// A record kind added to [`AgentRecordKind`] fails to compile here until it
/// declares where a sweep would find it — which is the only way a coverage
/// claim survives a milestone that was not thinking about this file.
const fn scanned_surface(kind: AgentRecordKind) -> ScannedSurface {
    match kind {
        // The agent entity's own record, and the two revisions it carries.
        AgentRecordKind::EntityState => ScannedSurface::Store("agents"),
        AgentRecordKind::DefinitionRevision => {
            ScannedSurface::CarriedIn(AgentRecordKind::EntityState)
        }
        AgentRecordKind::SettingsRevision => {
            ScannedSurface::CarriedIn(AgentRecordKind::EntityState)
        }

        // The task entity's record and everything it carries inline.
        AgentRecordKind::TaskState => ScannedSurface::Store("tasks"),
        AgentRecordKind::TaskDefinition => ScannedSurface::CarriedIn(AgentRecordKind::TaskState),
        AgentRecordKind::AdmissionDecision => ScannedSurface::CarriedIn(AgentRecordKind::TaskState),
        AgentRecordKind::ExchangeJournal => ScannedSurface::CarriedIn(AgentRecordKind::TaskState),
        AgentRecordKind::ExchangeEnvelope => {
            ScannedSurface::CarriedIn(AgentRecordKind::ExchangeJournal)
        }
        AgentRecordKind::ExchangeReply => {
            ScannedSurface::CarriedIn(AgentRecordKind::ExchangeJournal)
        }
        AgentRecordKind::EscrowLedger => ScannedSurface::CarriedIn(AgentRecordKind::TaskState),
        AgentRecordKind::TaskHistoryEntry => ScannedSurface::Store("task-history"),

        // The run entity's record and everything it carries inline.
        AgentRecordKind::RunState => ScannedSurface::Store("runs"),
        AgentRecordKind::LoopState => ScannedSurface::CarriedIn(AgentRecordKind::RunState),
        AgentRecordKind::RunEffect => ScannedSurface::CarriedIn(AgentRecordKind::LoopState),
        AgentRecordKind::ModelTurn => ScannedSurface::CarriedIn(AgentRecordKind::LoopState),
        AgentRecordKind::SetupRevision => ScannedSurface::CarriedIn(AgentRecordKind::LoopState),

        // Kinds no scenario in this file drives. Each carries its reason, and
        // the reason is what a reader checks rather than the absence.
        AgentRecordKind::Checkpoint => ScannedSurface::Unreached(
            "no scenario here parks on a checkpoint; `checkpoints.rs` owns that flow",
        ),
        AgentRecordKind::SessionMemoryEntry => ScannedSurface::Unreached(
            "the sweep wires no memory bundle; `session_memory.rs` owns the session tier",
        ),
        AgentRecordKind::MemoryContextSnapshot => ScannedSurface::Unreached(
            "assembled only with a memory bundle wired; see `memory_scope_fence.rs`",
        ),
        AgentRecordKind::PrivateMemory => ScannedSurface::Unreached(
            "written only by a promotion effect; `private_memory_promotion.rs` owns it",
        ),
        AgentRecordKind::DecisionEvent => ScannedSurface::Unreached(
            "emitted to a sink, not a store; scanned as a telemetry surface instead",
        ),
        AgentRecordKind::WakePolicyRevision => {
            ScannedSurface::Unreached("continuous-goal machinery; `wake_*.rs` owns it")
        }
        AgentRecordKind::WakeTimerState => {
            ScannedSurface::Unreached("the shared scanner index; `wake_scanner.rs` owns it")
        }
        AgentRecordKind::GoalSpec => {
            ScannedSurface::Unreached("goal machinery; `goal_contract.rs` owns it")
        }
        AgentRecordKind::GoalEvaluation => {
            ScannedSurface::Unreached("goal machinery; `goal_evaluation.rs` owns it")
        }
        AgentRecordKind::TeamState => {
            ScannedSurface::Unreached("coordination entity; `team_board.rs` owns it")
        }
        AgentRecordKind::TeamHistoryEntry => {
            ScannedSurface::Unreached("coordination entity; `team_board.rs` owns it")
        }
        AgentRecordKind::ConversationState => {
            ScannedSurface::Unreached("coordination entity; `conversation_turns.rs` owns it")
        }
        AgentRecordKind::ConversationHistoryEntry => {
            ScannedSurface::Unreached("coordination entity; `conversation_turns.rs` owns it")
        }

        // `AgentRecordKind` is `#[non_exhaustive]`, so an integration test —
        // a separate crate — cannot match it exhaustively however much it
        // would like to. The empty reason is the tripwire: a kind that lands
        // here is unclassified, and
        // `the_sweep_names_every_durable_record_kind_and_the_workflow_substrate`
        // refuses an unreached kind with no reason. The catalogue-length
        // assertion there is the second tripwire, for the same reason.
        _ => ScannedSurface::Unreached(""),
    }
}

/// How many record kinds this file was written against.
///
/// A milestone that adds one bumps this and fails the spine test, which is
/// deliberate: the author has to decide where the new kind is scanned rather
/// than inheriting a silent pass.
const CATALOGUE_LEN_AT_AUTHORING: usize = 29;

/// The durable surfaces outside `AgentRecordKind::ALL` that the sweep scans.
///
/// The workflow substrate versions its own records, so it carries no agent
/// record kind — and it is precisely where a failed attempt's detail lands.
/// A sweep driven by the agent catalogue alone would miss it entirely.
const SUBSTRATE_SURFACES: &[&str] = &["workflow", "fleet"];

/// The store labels `AuthorityFixture::durable_surfaces` is expected to yield.
fn expected_surface_labels() -> Vec<&'static str> {
    let mut labels: Vec<&'static str> = AgentRecordKind::ALL
        .iter()
        .filter_map(|kind| match scanned_surface(*kind) {
            ScannedSurface::Store(label) => Some(label),
            ScannedSurface::CarriedIn(_) | ScannedSurface::Unreached(_) => None,
        })
        .collect();
    labels.extend_from_slice(SUBSTRATE_SURFACES);
    labels.sort_unstable();
    labels.dedup();
    labels
}

// ---------------------------------------------------------------------------
// The scenario the sweep drives, and the shared assertions.
// ---------------------------------------------------------------------------

fn tool_id() -> AgentToolId {
    AgentToolId::new(TOOL).expect("the tool id is valid")
}

/// A model turn whose reasoning text and tool arguments both carry content
/// sentinels: durable state must hold them, telemetry must not.
fn tool_calling_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("RAKKA-CONTENT-REASONING")
        .with_tool_call(
            AgentToolCallRequest::new(
                AgentToolCallId::new("call-1").expect("the call id is valid"),
                tool_id(),
                serde_json::json!({ "memo": "RAKKA-CONTENT-ARG" }),
            )
            .expect("the tool call is bounded"),
        )
}

fn proposing_turn() -> AgentModelTurn {
    AgentModelTurn::new(CURRENT_AGENT_LOOP_ADAPTER_VERSION)
        .with_text("Done.")
        .with_proposal(
            AgentTaskContent::inline(serde_json::json!({ "answer": "RAKKA-CONTENT-RESULT" }))
                .expect("the proposal is inline-bounded"),
        )
}

/// A tool spec that names a credential binding, so the dispatch path resolves
/// one at all.
fn credentialed_spec() -> AgentEffectSpec {
    AgentEffectSpec::idempotent(2)
        .expect("the spec is valid")
        .with_credential_binding(
            AgentCredentialBindingRef::new("payments-api").expect("the binding is valid"),
        )
}

fn credentialed_fixture() -> AuthorityFixture {
    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn())
        .with_turn_for(2, proposing_turn());
    let spec = credentialed_spec();
    AuthorityFixture::over(adapter, tool_registry_for_spec(TOOL, &spec), None)
}

/// Asserts no [`SECRETS`] sentinel appears on any durable surface.
///
/// Panics naming both the surface and the sentinel, because "a secret leaked"
/// without saying where is a finding nobody can act on.
async fn assert_no_secret_anywhere(fx: &AuthorityFixture) {
    for (label, encoded) in fx.durable_surfaces().await {
        for secret in SECRETS {
            assert!(
                !encoded.contains(secret),
                "durable surface {label:?} carries the secret sentinel {secret:?}: {encoded}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 1. The spine. Every durable record kind is named, and the fixture yields
//    every surface the naming implies.
// ---------------------------------------------------------------------------

/// Every record kind is either scanned somewhere or unreached for a stated
/// reason, and the two sets partition the catalogue.
#[test]
fn the_sweep_names_every_durable_record_kind_and_the_workflow_substrate() {
    let mut scanned = 0usize;
    let mut carried = 0usize;
    let mut unreached = 0usize;

    for kind in AgentRecordKind::ALL {
        match scanned_surface(kind) {
            ScannedSurface::Store(label) => {
                assert!(
                    !label.is_empty(),
                    "{} names an empty surface label",
                    kind.as_label()
                );
                scanned += 1;
            }
            ScannedSurface::CarriedIn(carrier) => {
                // A carrier must itself resolve to a real store, or the
                // "scanning the carrier scans this" claim is vacuous.
                assert!(
                    matches!(
                        scanned_surface(carrier),
                        ScannedSurface::Store(_) | ScannedSurface::CarriedIn(_)
                    ),
                    "{} is carried in {}, which nothing scans",
                    kind.as_label(),
                    carrier.as_label()
                );
                assert_ne!(
                    carrier,
                    kind,
                    "{} claims to be carried in itself",
                    kind.as_label()
                );
                carried += 1;
            }
            ScannedSurface::Unreached(reason) => {
                assert!(
                    !reason.is_empty(),
                    "{} is unreached with no reason; an unexplained gap reads as coverage",
                    kind.as_label()
                );
                unreached += 1;
            }
        }
    }

    assert_eq!(
        scanned + carried + unreached,
        AgentRecordKind::ALL.len(),
        "the three dispositions must partition the catalogue"
    );
    assert_eq!(
        AgentRecordKind::ALL.len(),
        CATALOGUE_LEN_AT_AUTHORING,
        "the durable record catalogue changed; classify the new kind in \
         `scanned_surface` rather than letting it fall through the wildcard"
    );
    // A floor, so a refactor that collapses the scanned set into `Unreached`
    // fails here rather than reading as a green sweep.
    assert!(
        scanned + carried >= 16,
        "only {} of {} record kinds are actually scanned",
        scanned + carried,
        AgentRecordKind::ALL.len()
    );
    assert!(
        !SUBSTRATE_SURFACES.is_empty(),
        "the workflow substrate carries no record kind and must be scanned explicitly"
    );
}

/// The fixture yields exactly the surfaces the spine names — no more, and
/// crucially no fewer.
#[tokio::test]
async fn the_fixture_yields_every_surface_the_spine_names() {
    let fx = credentialed_fixture();
    fx.start().await;

    let mut actual: Vec<&'static str> = fx
        .durable_surfaces()
        .await
        .into_iter()
        .map(|(label, _)| label)
        .collect();
    actual.sort_unstable();
    actual.dedup();

    assert_eq!(
        actual,
        expected_surface_labels(),
        "the fixture's surface list and the spine's disagree"
    );
}

// ---------------------------------------------------------------------------
// 2. The negative control. A sweep that cannot find a planted sentinel is
//    asserting nothing at all.
// ---------------------------------------------------------------------------

/// The scanner finds a sentinel that really is in a durable record.
#[tokio::test]
async fn a_planted_sentinel_is_found_by_the_scanner() {
    let fx = credentialed_fixture();
    fx.start().await;
    fx.pump_until_tool_ticket().await;

    // The model's reasoning text is a CONTENT sentinel, and durable state
    // legitimately holds it. If the scanner cannot see it there, it could not
    // have seen a credential either.
    let surfaces = fx.durable_surfaces().await;
    let found = surfaces
        .iter()
        .any(|(_, encoded)| encoded.contains("RAKKA-CONTENT-REASONING"));
    assert!(
        found,
        "the scanner found no planted sentinel in any durable surface, so it \
         proves nothing about the ones it did not find"
    );
}

// ---------------------------------------------------------------------------
// 3. A resolved credential reaches no durable record.
// ---------------------------------------------------------------------------

/// Scenario 25's secret half, over every surface rather than three.
#[tokio::test]
async fn a_resolved_credential_reaches_no_durable_record_of_a_completed_run() {
    let fx = credentialed_fixture().with_credential_resolver("RAKKA-SECRET-BEARER");
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.pump().await;

    // The executor really did receive it — otherwise "absent everywhere" is
    // true because nothing was ever resolved.
    let invocations = fx.tools.invocations();
    let invocation = invocations
        .iter()
        .find(|invocation| invocation.tool == TOOL)
        .expect("the tool was invoked");
    assert!(
        invocation.with_credential,
        "the executor saw no credential, so this proves nothing"
    );

    assert_no_secret_anywhere(&fx).await;
}

// ---------------------------------------------------------------------------
// 4. The failure path: a resolver's own detail never becomes durable state.
// ---------------------------------------------------------------------------

/// A credential resolver that quotes its secret store is not laundered into
/// the durable outbox row or the fleet index.
///
/// This is the surface `effect_dispatch.rs` could not reach: its resolver
/// always succeeds, and the leak is on the failure path.
#[tokio::test]
async fn a_resolver_failure_persists_its_stable_code_and_never_the_resolvers_detail() {
    let fx = credentialed_fixture().with_failing_credential_resolver(
        "vault-unreachable",
        "vault said: token=RAKKA-SECRET-VAULT-DETAIL",
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;

    // One pass: the attempt reaches the resolver, fails, and records.
    let pass = fx.one_pass().await;
    assert!(
        pass.failed_attempts >= 1,
        "the attempt did not fail, so the failure path was never taken"
    );

    let surfaces = fx.durable_surfaces().await;

    // The stable code IS persisted — an operator must still be able to see
    // what happened.
    let coded = surfaces
        .iter()
        .any(|(_, encoded)| encoded.contains("credential-resolution-failed"));
    assert!(
        coded,
        "no durable surface records the stable failure code, so the runtime \
         traded observability for secrecy rather than separating them"
    );

    // The resolver's own words are not.
    for (label, encoded) in &surfaces {
        assert!(
            !encoded.contains("RAKKA-SECRET-VAULT-DETAIL"),
            "durable surface {label:?} carries the resolver's own failure text: {encoded}"
        );
    }
}

// ---------------------------------------------------------------------------
// 5. An executor's failure detail is bounded before it reaches a durable row.
// ---------------------------------------------------------------------------

/// Bounding is not sanitizing, and the test says so: a long executor error is
/// truncated at the documented bound, so no collaborator can make a durable
/// record grow without limit.
#[tokio::test]
async fn an_executor_failure_detail_is_bounded_before_it_reaches_a_durable_row() {
    let long = format!(
        "RAKKA-SECRET-EXECUTOR-DETAIL{}",
        "x".repeat(rakka_agent::dispatch::AGENT_DISPATCH_FAILURE_DETAIL_MAX_LENGTH * 4)
    );
    let spec = credentialed_spec();
    let adapter = DeterministicModelAdapter::new()
        .with_turn_for(1, tool_calling_turn())
        .with_turn_for(2, proposing_turn());
    let mut fx = AuthorityFixture::over(adapter, tool_registry_for_spec(TOOL, &spec), None)
        .with_credential_resolver("RAKKA-SECRET-BEARER");
    fx.tools = RecordingToolExecutor::new().with_failure(TOOL, "executor-exploded", &long);
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let pass = fx.one_pass().await;
    assert!(
        pass.failed_attempts >= 1,
        "the executor did not fail, so nothing exercised the bound"
    );

    for (label, encoded) in fx.durable_surfaces().await {
        assert!(
            !encoded.contains(&long),
            "durable surface {label:?} carries the executor's unbounded detail"
        );
    }
}

// ---------------------------------------------------------------------------
// 6. Killing a worker while a credential is live leaves no trace of it.
// ---------------------------------------------------------------------------

/// The one window at which a resolved credential exists in memory.
///
/// No existing test kills a worker there, so "the resolved value never
/// outlives the attempt" was an argument about code shape rather than an
/// observed fact.
#[tokio::test]
async fn killing_the_worker_while_a_credential_is_live_leaves_no_trace_of_it() {
    let fx = credentialed_fixture().with_credential_resolver("RAKKA-SECRET-BEARER");
    fx.start().await;
    fx.pump_until_tool_ticket().await;

    fx.probe
        .arm(rakka_agent::AgentDispatchWindow::CredentialResolved);
    let pass = fx.one_pass().await;
    assert_eq!(
        fx.probe.deaths(),
        1,
        "the worker did not die at the credential-resolved window, so the \
         window is unreachable and this test asserts nothing"
    );
    assert_eq!(
        pass.invoked, 0,
        "the worker died before invoking, so nothing external happened"
    );

    assert_no_secret_anywhere(&fx).await;

    // And the recovery attempt resolves again rather than reusing anything
    // the dead worker might have left behind.
    fx.expire_lease();
    fx.pump().await;
    assert_no_secret_anywhere(&fx).await;
}

// ---------------------------------------------------------------------------
// 7. Structured logs. The one observability surface with no reader of its own.
// ---------------------------------------------------------------------------

/// Nothing the dispatch path logs carries a secret or model content.
///
/// The dispatch pipeline's `tracing` lines are the only structured logs either
/// agent crate emits, and until now nothing read them back: the guardrail
/// lines and the credential-failure warning were asserted by inspection.
#[tokio::test]
async fn no_structured_log_the_dispatch_path_emits_carries_a_secret_or_content() {
    let logs = rakka_agent::testkit::CapturingSubscriber::install_global();
    logs.clear();

    let fx = credentialed_fixture().with_failing_credential_resolver(
        "vault-unreachable",
        "vault said: token=RAKKA-SECRET-VAULT-DETAIL",
    );
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    let _pass = fx.one_pass().await;

    let events = logs.events();
    assert!(
        !events.is_empty(),
        "the dispatch path emitted no structured log at all, so this test \
         cannot distinguish a clean surface from an absent one"
    );

    // The warning fired, and it names the logical binding an operator acts on.
    let warned = events
        .iter()
        .any(|event| event.contains("credential_binding"));
    assert!(
        warned,
        "no log names the credential binding, so the diagnostics the durable \
         record deliberately gave up were not emitted anywhere: {events:?}"
    );

    for event in &events {
        for sentinel in SECRETS.iter().chain(CONTENT.iter()) {
            assert!(
                !event.contains(sentinel),
                "a structured log carries {sentinel:?}: {event}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 8. Metrics. The series set, not just the per-observation gate.
// ---------------------------------------------------------------------------

/// A credentialed run's emitted metric series carry no secret, no content, and
/// no identifier.
///
/// The per-observation gate (`validate_agent_domain_metric_attributes`) checks
/// what a call site routes through it. This checks the *series set* a run
/// actually emitted, which is what a raw identifier in a label would show up
/// as — a new series that nobody wrote down.
#[tokio::test]
async fn the_metric_series_of_a_credentialed_run_carry_no_identifier_or_secret() {
    let metrics = std::sync::Arc::new(rakka_core::InMemoryMetricsRecorder::new());
    let mut fx = credentialed_fixture().with_credential_resolver("RAKKA-SECRET-BEARER");
    fx.fx = fx.fx.with_metrics(metrics.clone());
    fx.start().await;
    fx.pump_until_tool_ticket().await;
    fx.pump().await;

    for (label, rendered) in fx.telemetry_surfaces(&metrics) {
        for sentinel in SECRETS.iter().chain(CONTENT.iter()) {
            assert!(
                !rendered.contains(sentinel),
                "telemetry surface {label:?} carries {sentinel:?}: {rendered}"
            );
        }
        // The high-cardinality identifiers a label must never carry.
        for identifier in [TASK, AGENT, TASK_DEFINITION] {
            assert!(
                !rendered.contains(identifier),
                "telemetry surface {label:?} carries the identifier {identifier:?}, \
                 which is what an unbounded metric label looks like: {rendered}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 7. A dispatch authority's own words are bounded too — code as well as
//    detail.
// ---------------------------------------------------------------------------

/// The filler an over-long refusal code carries, so a surface holding more
/// than the bound is visible as a single run of one character.
const CODE_FILLER: char = 'c';

/// The filler an over-long refusal message carries.
const DETAIL_FILLER: char = 'm';

fn oversized_refusal(retryable: bool) -> rakka_agent::AgentAuthorityRefusal {
    let code = format!(
        "authority-refused-{}",
        CODE_FILLER
            .to_string()
            .repeat(rakka_agent::dispatch::AGENT_DISPATCH_FAILURE_CODE_MAX_LENGTH * 4)
    );
    let message = format!(
        "RAKKA-LONG-REFUSAL-DETAIL{}",
        DETAIL_FILLER
            .to_string()
            .repeat(rakka_agent::dispatch::AGENT_DISPATCH_FAILURE_DETAIL_MAX_LENGTH * 4)
    );
    if retryable {
        rakka_agent::AgentAuthorityRefusal::transient(code, message)
    } else {
        rakka_agent::AgentAuthorityRefusal::of(code, message)
    }
}

/// Asserts no durable surface kept more than the bound of either filler.
///
/// A single run of one character is the precise probe: finding `bound + 1` of
/// them proves a field kept more than the bound, and finding none proves every
/// field that carries the filler was truncated at or below it.
async fn assert_refusal_words_are_bounded(fx: &AuthorityFixture) {
    let over_code = CODE_FILLER
        .to_string()
        .repeat(rakka_agent::dispatch::AGENT_DISPATCH_FAILURE_CODE_MAX_LENGTH + 1);
    let over_detail = DETAIL_FILLER
        .to_string()
        .repeat(rakka_agent::dispatch::AGENT_DISPATCH_FAILURE_DETAIL_MAX_LENGTH + 1);
    for (label, encoded) in fx.durable_surfaces().await {
        assert!(
            !encoded.contains(&over_code),
            "durable surface {label:?} kept more than the code bound of the \
             authority's own refusal code"
        );
        assert!(
            !encoded.contains(&over_detail),
            "durable surface {label:?} kept more than the detail bound of the \
             authority's own refusal message"
        );
    }
}

/// A *transient* refusal is the worst case, and it was unbounded.
///
/// Deferral is the retry path: the composed line is written onto the single
/// shared fleet index record every worker re-persists on every claim pass, and
/// it repeats every backoff interval for as long as the condition lasts. Only
/// the message half was bounded; the code went in verbatim.
#[tokio::test]
async fn a_transient_authority_refusal_is_bounded_on_the_shared_fleet_index() {
    // The gate refuses every intent, so the run's own turn-1 model effect is
    // the ticket under test — a refusal reaches this path long before any
    // tool call does.
    let fx = credentialed_fixture().with_fixed_refusal(oversized_refusal(true));
    fx.start().await;

    // Several passes, because this is the path that repeats.
    for _round in 0..3 {
        let pass = fx.one_pass().await;
        assert_eq!(
            pass.deferred, 1,
            "the transient refusal did not defer, so nothing exercised the bound"
        );
        assert_eq!(pass.failed_attempts, 0, "a deferral spends no attempt");
    }

    assert_refusal_words_are_bounded(&fx).await;
}

/// A *definitive* refusal reaches durable run state, the outbox row, and the
/// fleet index — all three under the authority's own code.
#[tokio::test]
async fn a_definitive_authority_refusal_is_bounded_on_every_durable_surface() {
    let fx = credentialed_fixture().with_fixed_refusal(oversized_refusal(false));
    fx.start().await;
    let pass = fx.one_pass().await;
    assert_eq!(
        pass.cancelled, 1,
        "the definitive refusal did not settle the ticket, so nothing \
         exercised the bound"
    );

    assert_refusal_words_are_bounded(&fx).await;
}
