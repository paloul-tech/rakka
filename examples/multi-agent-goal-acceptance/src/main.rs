//! Prints the acceptance transcript, one stable line per milestone bullet.

#[tokio::main]
async fn main() {
    let report = rakka_example_multi_agent_goal_acceptance::run_acceptance().await;
    for line in &report.lines {
        println!("{line}");
    }
}
