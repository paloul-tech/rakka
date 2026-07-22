//! The acceptance walk, asserted: the transcript the binary prints is
//! exactly the one the README documents, and the typed facts behind every
//! line hold. `cargo test --workspace` runs this, so the documented stdout
//! cannot rot.

use rakka_example_durable_agent_acceptance::report::EXPECTED_TRANSCRIPT;
use rakka_example_durable_agent_acceptance::run_acceptance;

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
    assert_eq!(report.tool_invocations, 1, "invoked exactly once");
    assert_eq!(report.tool_idempotency_keys, 1, "one external key");
    assert!(report.session_entries > 0);
    assert!(report.context_snapshots > 0);
    assert!(report.decisions_owed > 0);
    assert!(report.trace_segments > 0);
    assert!(report
        .metric_names
        .iter()
        .all(|name| name.starts_with("rakka.agent.")));

    // Scenario 25's discipline, applied to the example: no telemetry surface
    // carries model text, tool arguments, or credential material.
    for surface in &report.telemetry_surfaces {
        for sentinel in [
            "SENSITIVE-REASONING",
            "SECRET-TOKEN",
            "charged and resolved",
        ] {
            assert!(
                !surface.contains(sentinel),
                "{sentinel} leaked into a telemetry surface"
            );
        }
    }
}
