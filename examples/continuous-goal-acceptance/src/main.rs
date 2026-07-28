//! Prints the M3 acceptance walk's transcript, one line per milestone fact.

#![forbid(unsafe_code)]

#[tokio::main]
async fn main() {
    let report = rakka_example_continuous_goal_acceptance::run_acceptance().await;
    for line in &report.lines {
        println!("{line}");
    }
}
