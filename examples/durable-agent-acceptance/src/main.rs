//! Prints the M1 acceptance walk's transcript, one line per spec 22 bullet.
//!
//! With `--provider`, prints the gated live-provider walk's facts instead, or
//! the gate variable that is unset — exit 2 — when the walk is skipped.

#![forbid(unsafe_code)]

#[tokio::main]
async fn main() {
    if std::env::args().any(|arg| arg == "--provider") {
        match rakka_example_durable_agent_acceptance::run_provider_walk().await {
            Ok(report) => {
                for line in &report.lines {
                    println!("{line}");
                }
            }
            Err(reason) => {
                eprintln!("{reason}");
                std::process::exit(2);
            }
        }
        return;
    }
    let report = rakka_example_durable_agent_acceptance::run_acceptance().await;
    for line in &report.lines {
        println!("{line}");
    }
}
