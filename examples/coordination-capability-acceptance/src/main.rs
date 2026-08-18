//! Prints the acceptance transcript, one stable line per milestone bullet.

#![forbid(unsafe_code)]

#[tokio::main]
async fn main() {
    let report = rakka_example_coordination_capability_acceptance::run_acceptance().await;
    for line in &report.lines {
        println!("{line}");
    }
}
