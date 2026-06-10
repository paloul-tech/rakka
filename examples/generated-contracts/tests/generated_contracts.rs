//! End-to-end generated-contract example tests.

use rakka_example_generated_contracts::{run_generated_contract_demo_with_options, DemoOptions};

#[tokio::test]
async fn generated_grpc_and_http_contracts_round_trip() {
    let options = DemoOptions::new(env!("CARGO_BIN_EXE_rakka-example-generated-contract-child"));
    let report = run_generated_contract_demo_with_options(options)
        .await
        .expect("generated contracts should run end to end");

    assert_eq!(report.grpc_counter_value, 7);
    assert_eq!(report.grpc_cart_sku, "book");
    assert_eq!(
        report.grpc_catalog_items,
        ["book".to_owned(), "box".to_owned()]
    );
    assert_eq!(report.grpc_ingested_count, 2);
    assert_eq!(report.grpc_bidi_ack_count, 2);
    assert_eq!(report.grpc_workflow_revision, 1);
    assert_eq!(report.grpc_legacy_result, 42);
    assert_eq!(report.http_counter_value, 12);
    assert_eq!(report.http_cart_sku, "pencil");
    assert_eq!(report.http_binary_counter_value, 23);
    assert_eq!(report.http_workflow_revision, 2);
    assert_eq!(report.http_legacy_result, 100);
    assert!(report
        .cart_events
        .iter()
        .any(|event| event == "grpc-client-stream:paper"));
}
