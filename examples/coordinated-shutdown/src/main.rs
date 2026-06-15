#![forbid(unsafe_code)]

//! Runnable example for Phase 7 coordinated shutdown.

use std::io::Read;
use std::time::Duration;

use rakka::http::{register_http_shutdown_task, HttpShutdownHandle};
use rakka::prelude::{
    actor_fn, ActorAction, ActorContext, ActorSystem, RakkaError, ShutdownOutcome, ShutdownPhase,
    ShutdownTaskStatus,
};
use rakka::process::{
    register_configured_process_actor_stop_task, spawn_process_actor, ExecutableAllowlist,
    ProcessActorCommand, ProcessActorConfig, ProcessActorState, ProcessSpec, ProcessStdio,
};
use rakka::stream::{bounded_channel, register_stream_sink_drain, StreamLifecycle};

const CHILD_FLAG: &str = "--shutdown-child";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
enum AuditCommand {
    Record(&'static str),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == CHILD_FLAG) {
        run_child_process()?;
        return Ok(());
    }

    let system = ActorSystem::new("coordinated-shutdown-example");
    let shutdown = system.coordinated_shutdown();

    let audit = system.spawn(
        "audit",
        actor_fn(|_ctx: &mut ActorContext<AuditCommand>, msg| match msg {
            AuditCommand::Record(message) => {
                println!("audit actor recorded {message}");
                Ok(ActorAction::Continue)
            }
        }),
    )?;
    audit
        .tell(AuditCommand::Record("startup"))
        .map_err(|error| RakkaError::core("startup-audit-send", format!("{error:?}")))?;

    let http = HttpShutdownHandle::new();
    register_http_shutdown_task(&shutdown, "stop-public-http", http.clone())?;
    let http_waiter = tokio::spawn(http.signal().wait());

    let (orders_sink, orders_source) = bounded_channel::<String>(4)?;
    orders_sink
        .send("order-1".to_owned())
        .await
        .map_err(|error| {
            let (error, _item) = error.into_parts();
            error.into_rakka_error()
        })?;
    register_stream_sink_drain(&shutdown, "drain-orders-stream", orders_sink.clone())?;

    let process_config = process_actor_config()?;
    let process_actor = spawn_process_actor(&system, "cooperative-child", process_config.clone())?;
    let process_status = process_actor
        .ask(
            |reply_to| ProcessActorCommand::Start { reply_to },
            DEFAULT_TIMEOUT,
        )
        .await??;
    assert_eq!(process_status.state(), ProcessActorState::Running);
    register_configured_process_actor_stop_task(
        &shutdown,
        "stop-cooperative-child",
        process_actor,
        &process_config,
    )?;

    shutdown.add_task(ShutdownPhase::flush_persistence(), "publish-final-audit", {
        let audit = audit.clone();
        move |_context| {
            let audit = audit.clone();
            async move {
                audit
                    .tell(AuditCommand::Record("final-audit"))
                    .map_err(|error| RakkaError::core("final-audit-send", format!("{error:?}")))?;
                Ok(())
            }
        }
    })?;

    let report = system.terminate_with_report().await?;
    system.when_terminated().await;
    http_waiter.await?;

    assert_eq!(report.outcome(), ShutdownOutcome::Complete);
    assert_task_completed(&report, ShutdownPhase::stop_ingress(), "stop-public-http");
    assert_task_completed(
        &report,
        ShutdownPhase::drain_adapters(),
        "drain-orders-stream",
    );
    assert_task_completed(
        &report,
        ShutdownPhase::stop_process_actors(),
        "stop-cooperative-child",
    );
    assert_task_completed(
        &report,
        ShutdownPhase::flush_persistence(),
        "publish-final-audit",
    );
    assert!(http.snapshot().shutdown_requested());
    assert_eq!(
        orders_source.status().lifecycle(),
        StreamLifecycle::Draining
    );
    assert_eq!(orders_source.next().await?, Some("order-1".to_owned()));
    assert_eq!(orders_source.next().await?, None);

    println!(
        "Coordinated shutdown completed {} phases and drained HTTP, streams, process actors, and custom tasks.",
        report.phases().len()
    );
    Ok(())
}

fn process_actor_config() -> Result<ProcessActorConfig, std::io::Error> {
    let executable = std::env::current_exe()?;
    let allowlist = ExecutableAllowlist::from_exact_paths([executable.clone()]);
    let spec = ProcessSpec::new(executable)
        .arg(CHILD_FLAG)
        .stdin(ProcessStdio::Piped)
        .shutdown_timeout(Duration::from_millis(500));
    Ok(ProcessActorConfig::new(spec, allowlist))
}

fn assert_task_completed(
    report: &rakka::prelude::CoordinatedShutdownReport,
    phase: ShutdownPhase,
    task_name: &str,
) {
    let status = report
        .phases()
        .iter()
        .find(|phase_report| phase_report.phase() == &phase)
        .and_then(|phase_report| {
            phase_report
                .tasks()
                .iter()
                .find(|task| task.task_name() == task_name)
        })
        .map(|task| task.status());
    assert_eq!(status, Some(ShutdownTaskStatus::Completed));
}

fn run_child_process() -> Result<(), Box<dyn std::error::Error>> {
    let mut stdin = std::io::stdin();
    let mut buffer = String::new();
    let _bytes = stdin.read_to_string(&mut buffer)?;
    Ok(())
}
