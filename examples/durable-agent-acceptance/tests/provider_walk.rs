//! The live-provider walk, gated: with no `RAKKA_MODEL_PROFILE` this test
//! announces the skip and passes; with the gate set it drives the world's
//! run through the real Rig provider adapter and asserts the structural
//! facts a live model can be held to.
//!
//! One of those facts is provider-conditional. Rig 0.37 carries a provider's
//! own response metadata back only where the adapter knows its raw response
//! type — Anthropic, OpenAI Chat Completions, and a custom endpoint of that
//! shape — so the response model is asserted present for those kinds and
//! absent for every other, which holds the *mapping* rather than the model.
//! The terminal line and the secret exclusion hold for every kind.

use rakka_example_durable_agent_acceptance::provider::provider_reports_its_model;
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
            if provider_reports_its_model(&report.provider) {
                assert!(
                    report.response_model.is_some(),
                    "a live provider reports the model that answered"
                );
            } else {
                assert!(
                    report.response_model.is_none(),
                    "{} carries no response provenance through rig 0.37, so a reported \
                     model could only have been invented: {:?}",
                    report.provider.as_label(),
                    report.response_model
                );
                eprintln!(
                    "note: {} carries no response provenance through rig 0.37; the \
                     response model is unreported by design",
                    report.provider.as_label()
                );
            }
            let key = std::env::var("RAKKA_MODEL_API_KEY").unwrap_or_default();
            if !key.is_empty() {
                for line in &report.lines {
                    assert!(!line.contains(&key), "the key never reaches the transcript");
                }
            }
        }
    }
}
