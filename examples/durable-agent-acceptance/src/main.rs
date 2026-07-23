//! Prints the M1 acceptance walk's transcript, one line per spec 22 bullet.

#![forbid(unsafe_code)]

#[tokio::main]
async fn main() {
    let report = rakka_example_durable_agent_acceptance::run_acceptance().await;
    for line in &report.lines {
        println!("{line}");
    }
}
