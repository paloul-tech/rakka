//! The live-provider walk, gated: with no `RAKKA_MODEL_PROFILE` this test
//! announces the skip and passes; with the gate set it drives the world's
//! run through the real Rig provider adapter and asserts the structural
//! facts a live model can be held to.

use rakka_example_durable_agent_acceptance::run_provider_walk;

#[tokio::test]
async fn the_provider_walk_is_gated_and_holds_its_facts_when_armed() {
    match run_provider_walk().await {
        Err(reason) => {
            assert!(
                reason.contains("RAKKA_MODEL_PROFILE"),
                "the skip names its gate: {reason}"
            );
            eprintln!("skipped: {reason}");
        }
        Ok(report) => {
            assert!(
                report
                    .lines
                    .iter()
                    .any(|line| line.starts_with("ok  terminal:")),
                "{:?}",
                report.lines
            );
            assert!(
                report.response_model.is_some(),
                "a live provider reports the model that answered"
            );
            let key = std::env::var("RAKKA_MODEL_API_KEY").unwrap_or_default();
            if !key.is_empty() {
                for line in &report.lines {
                    assert!(!line.contains(&key), "the key never reaches the transcript");
                }
            }
        }
    }
}
