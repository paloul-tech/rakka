//! The acceptance walk, asserted: the transcript the binary prints is
//! exactly the one the README documents, and the typed facts behind every
//! line hold. `cargo test --workspace` runs this, so the documented stdout
//! cannot rot.

use rakka_example_continuous_goal_acceptance::report::EXPECTED_TRANSCRIPT;
use rakka_example_continuous_goal_acceptance::run_acceptance;

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
        report.epoch_tasks, 9,
        "one durable epoch task per admission"
    );
    assert_eq!(report.admitted, 9);
    assert_eq!(report.coalesced, 1);
    assert_eq!(report.missed, 2);
    assert_eq!(report.fenced, 1);
    assert_eq!(report.deferred, 1);
    assert_eq!(report.backed_off, 1);
    assert_eq!(report.retried, 2, "one backoff retry, one window turn");
    assert_eq!(report.barred, 1);
    assert_eq!(report.stale_owner_code, "revision-conflict");
    assert_eq!(report.renewed_expiry, 50_000_000);
    assert_eq!(report.escrow_outstanding, 0, "every epoch's budget settled");
    assert!(!report.pending_wake, "nothing is pending after retirement");
}
