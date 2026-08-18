//! The acceptance walk, asserted: the transcript the binary prints is
//! exactly the one the README documents, and the typed facts behind every
//! line hold. `cargo test --workspace` runs this, so the documented stdout
//! cannot rot.

use rakka_example_coordination_capability_acceptance::report::EXPECTED_TRANSCRIPT;
use rakka_example_coordination_capability_acceptance::{run_acceptance, CONTENT_SENTINELS};

#[test]
fn the_readme_transcript_matches_the_const() {
    // The README quotes the transcript by hand; this extraction is what makes
    // "a single source for all three" true rather than aspirational.
    let readme = include_str!("../README.md");
    let section = readme
        .split("## Expected stdout")
        .nth(1)
        .expect("the README has an Expected stdout section");
    let block = section
        .split("```text\n")
        .nth(1)
        .and_then(|rest| rest.split("\n```").next())
        .expect("the section carries a fenced transcript");
    assert_eq!(
        block.lines().collect::<Vec<_>>(),
        EXPECTED_TRANSCRIPT,
        "the README's Expected stdout block drifted from EXPECTED_TRANSCRIPT"
    );
}

#[tokio::test]
async fn the_transcript_is_exactly_the_documented_one() {
    let report = run_acceptance().await;
    assert_eq!(
        report.lines,
        EXPECTED_TRANSCRIPT
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        "the walk's transcript drifted from the documented one; update the README, the \
         EXPECTED_TRANSCRIPT const, and this walk together"
    );

    // The typed facts behind the lines.
    assert_eq!(
        report.task_id, "ticket-4711",
        "one task id, start to finish"
    );
    assert_eq!(
        report.generations, 2,
        "the claim bought generation 1 and the transfer bought generation 2 — no more"
    );
    assert_eq!(report.claim_refusal_code, "team-coordination-unauthorized");
    assert_eq!(
        report.turn_refusal_code,
        "conversation-moderation-unauthorized"
    );
    assert_eq!(
        report.resident_at_wait, 0,
        "an idle team and its members hold no runtime resource"
    );
    assert_eq!(report.owner_after_handoff, "billing-agent");
    assert_eq!(
        report.source_status, "handed-off",
        "the source terminalized, and only after the target's acceptance"
    );
    assert_eq!(
        report.transfers_attempted, 1,
        "one transfer, however many times the flow re-drove"
    );
    assert_eq!(report.human_results_accepted, 1);
    assert!(report.dependent_unblocked);
    assert!(
        report.checkpoint_gated_effect,
        "the consequential tool is checkpoint-bound by declaration"
    );
    assert_eq!(
        report.effect_invocations, 0,
        "a human task is not a substitute for an effect-bound checkpoint"
    );
    assert_eq!(report.turns_recorded, 1);
    assert_eq!(
        report.turns_after_recovery, 2,
        "the re-driven turn recorded exactly once across the owner's death"
    );
    assert_eq!(report.conversation_terminal, "moderator-ended");
    assert_eq!(report.board_entry_status, "done");
    assert!(
        report.replayed_events > 0,
        "the walk left a coordination log to replay"
    );
    assert!(report.window_expired_resumed);

    // The no-leak sweep, re-run over the reported surfaces so the report
    // itself cannot drift from the walk's own assertion.
    assert!(!report.surfaces.is_empty());
    for surface in &report.surfaces {
        for sentinel in CONTENT_SENTINELS {
            assert!(
                !surface.contains(sentinel),
                "a coordination surface leaked {sentinel}"
            );
        }
    }
}
