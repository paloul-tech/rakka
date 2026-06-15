//! Coordinated shutdown tests for persistence hooks.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rakka_core::{
    CoordinatedShutdown, CoordinatedShutdownReason, CoordinatedShutdownReport, ShutdownPhase,
    ShutdownTaskStatus,
};
use rakka_persistence::{
    register_persistence_flush_task, register_persistence_query_cancel_task,
    register_persistence_shutdown_task, InMemoryEventJournal,
};
use rakka_stream::{bounded_channel, StreamError};

#[tokio::test]
async fn in_memory_persistence_registers_noop_flush_task() {
    let shutdown = CoordinatedShutdown::new();
    let journal = InMemoryEventJournal::<String>::new();

    let task = register_persistence_shutdown_task(&shutdown, "flush-memory-journal", journal)
        .expect("memory persistence shutdown task should register");

    assert_eq!(task.phase(), &ShutdownPhase::flush_persistence());
    assert!(task
        .options()
        .attributes()
        .iter()
        .any(|attribute| attribute.key() == "operation" && attribute.value() == "noop-flush"));
    assert!(task
        .options()
        .attributes()
        .iter()
        .any(|attribute| attribute.key() == "backend" && attribute.value() == "memory"));

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .expect("memory persistence shutdown should complete");

    assert_eq!(
        task_status(
            &report,
            ShutdownPhase::flush_persistence(),
            "flush-memory-journal"
        ),
        Some(ShutdownTaskStatus::Completed)
    );
}

#[tokio::test]
async fn query_stream_cancel_runs_before_persistence_flush() {
    let shutdown = CoordinatedShutdown::new();
    let (sink, source) = bounded_channel::<u64>(4).expect("query stream should allocate");
    sink.try_send(1).expect("query stream should accept item");
    let observed_cancel = Arc::new(AtomicBool::new(false));

    register_persistence_query_cancel_task(
        &shutdown,
        "cancel-query-stream",
        source.clone(),
        "coordinated-shutdown",
    )
    .expect("query cancel task should register");

    register_persistence_flush_task(
        &shutdown,
        "flush-after-query-cancel",
        "custom",
        Some("counter|1".to_owned()),
        {
            let source = source.clone();
            let observed_cancel = observed_cancel.clone();
            move || {
                let source = source.clone();
                let observed_cancel = observed_cancel.clone();
                async move {
                    match source.next().await {
                        Err(StreamError::Cancelled { reason }) => {
                            assert_eq!(reason.as_deref(), Some("coordinated-shutdown"));
                            observed_cancel.store(true, Ordering::SeqCst);
                            Ok(())
                        }
                        other => Err(rakka_core::RakkaError::new(
                            rakka_core::Subsystem::Persistence,
                            "query-not-cancelled",
                            format!("expected cancelled query stream, got {other:?}"),
                        )),
                    }
                }
            }
        },
    )
    .expect("persistence flush task should register");

    let report = shutdown
        .run(CoordinatedShutdownReason::user_request())
        .await
        .expect("query cancellation and flush should complete");

    assert!(observed_cancel.load(Ordering::SeqCst));
    assert_eq!(
        task_status(
            &report,
            ShutdownPhase::drain_adapters(),
            "cancel-query-stream"
        ),
        Some(ShutdownTaskStatus::Completed)
    );
    assert_eq!(
        task_status(
            &report,
            ShutdownPhase::flush_persistence(),
            "flush-after-query-cancel"
        ),
        Some(ShutdownTaskStatus::Completed)
    );
}

fn task_status(
    report: &CoordinatedShutdownReport,
    phase: ShutdownPhase,
    task_name: &str,
) -> Option<ShutdownTaskStatus> {
    report
        .phases()
        .iter()
        .find(|phase_report| phase_report.phase() == &phase)?
        .tasks()
        .iter()
        .find(|task_report| task_report.task_name() == task_name)
        .map(|task_report| task_report.status())
}
