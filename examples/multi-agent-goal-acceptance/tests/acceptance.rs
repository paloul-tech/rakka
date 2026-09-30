//! The acceptance walk, asserted: the transcript the binary prints is
//! exactly the one the README documents, and the typed facts behind every
//! line hold. `cargo test --workspace` runs this, so the documented stdout
//! cannot rot.

use rakka_example_multi_agent_goal_acceptance::report::EXPECTED_TRANSCRIPT;
use rakka_example_multi_agent_goal_acceptance::{run_acceptance, CONTENT_SENTINELS};

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
    assert_eq!(report.child_tasks.len(), 2, "two specialist children");
    assert_ne!(report.child_tasks[0], report.child_tasks[1]);
    assert!(
        report.invocation_id.starts_with("workflow-invocation-"),
        "the invocation id is derived"
    );
    assert_eq!(report.inbox_start_entries, 1, "one durable StartRun ever");
    assert_eq!(
        report.refund_step_executions, 1,
        "the compiled step ran once"
    );
    assert_eq!(report.tool_invocations, 1, "invoked exactly once");
    assert_eq!(report.tool_idempotency_keys, 1, "one external key");
    assert_eq!(report.resident_at_wait, 0, "the wait held nothing resident");
    assert_eq!(report.unattested_code, "task-goal-decision-unattested");
    assert_eq!(report.goal_status, "satisfied");
    assert!(report.claim_provenance_has_delegation);
    assert_eq!(report.view_tasks, 3);
    assert_eq!(report.view_runs, 3);
    assert_eq!(report.view_delegations, 2);
    assert_eq!(report.view_workflow_links, 1);
    assert_eq!(report.view_evaluations, 1);
    assert!(report.view_evidence >= 1);
    assert_eq!(report.view_claims, 1);
    assert_eq!(report.escrow_outstanding, 0);

    // The no-leak sweep, re-run over the reported surfaces so the report
    // itself cannot drift from the walk's own assertion.
    assert!(!report.surfaces.is_empty());
    for surface in &report.surfaces {
        for sentinel in CONTENT_SENTINELS {
            assert!(
                !surface.contains(sentinel),
                "a reported surface leaked {sentinel}"
            );
        }
    }
}
