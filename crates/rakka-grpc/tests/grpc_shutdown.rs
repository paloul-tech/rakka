//! gRPC coordinated shutdown helper tests.

use std::time::Duration;

use rakka_core::{CoordinatedShutdown, CoordinatedShutdownReason, ShutdownOutcome};
use rakka_grpc::{register_grpc_shutdown_task, GrpcServerShutdownResult, GrpcShutdownHandle};

#[tokio::test]
async fn coordinated_shutdown_task_requests_grpc_shutdown_signal() {
    let shutdown = CoordinatedShutdown::new();
    let handle = GrpcShutdownHandle::new();

    register_grpc_shutdown_task(&shutdown, "stop-public-grpc", handle.clone()).unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(10), handle.signal().wait())
            .await
            .is_err(),
        "signal should wait before shutdown runs"
    );
    let waiter = tokio::spawn(handle.signal().wait());

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .unwrap();

    waiter.await.unwrap();
    assert_eq!(report.outcome(), ShutdownOutcome::Complete);
    assert!(handle.is_shutdown_requested());
    assert!(handle.snapshot().shutdown_requested());
}

#[test]
fn grpc_shutdown_handle_records_server_result() {
    let handle = GrpcShutdownHandle::new();

    handle.record_server_completed();
    assert_eq!(
        handle.snapshot().server_result(),
        Some(&GrpcServerShutdownResult::Completed)
    );

    handle.record_server_failed("serve failed");
    assert_eq!(
        handle.snapshot().server_result(),
        Some(&GrpcServerShutdownResult::Failed {
            message: "serve failed".to_owned()
        })
    );
}
