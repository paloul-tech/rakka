//! Prints the telemetry export acceptance transcript, one line per claim.

#![forbid(unsafe_code)]

#[tokio::main]
async fn main() {
    let report = rakka_example_agent_otlp_export_acceptance::run_acceptance().await;
    for line in &report.lines {
        println!("{line}");
    }
}
