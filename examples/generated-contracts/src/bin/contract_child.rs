#![forbid(unsafe_code)]

//! Line-json legacy child process for generated-contract tests.

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    rakka_example_generated_contracts::run_legacy_child()
}
