#![forbid(unsafe_code)]

//! Generated-contract example binary.

use rakka_example_generated_contracts::{run_generated_contract_demo, run_legacy_child};

const LEGACY_CHILD_FLAG: &str = "--generated-contract-legacy-child";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if std::env::args().any(|arg| arg == LEGACY_CHILD_FLAG) {
        run_legacy_child()?;
        return Ok(());
    }

    let report = run_generated_contract_demo().await?;
    println!(
        "Generated gRPC CounterService returned value {}.",
        report.grpc_counter_value
    );
    println!(
        "Generated gRPC CartService accepted {} and CatalogService returned {:?}.",
        report.grpc_cart_sku, report.grpc_catalog_items
    );
    println!(
        "Generated gRPC streaming accepted {} upload item(s) and {} bidi ack(s).",
        report.grpc_ingested_count, report.grpc_bidi_ack_count
    );
    println!(
        "Generated gRPC WorkflowService revision {} and LegacyService result {}.",
        report.grpc_workflow_revision, report.grpc_legacy_result
    );
    println!(
        "Mirrored HTTP JSON returned counter {}, cart {}, workflow revision {}, legacy {}; binary counter {}.",
        report.http_counter_value,
        report.http_cart_sku,
        report.http_workflow_revision,
        report.http_legacy_result,
        report.http_binary_counter_value
    );
    Ok(())
}
