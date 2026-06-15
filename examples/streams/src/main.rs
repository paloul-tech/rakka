#![forbid(unsafe_code)]

//! Runnable examples for the Phase 6 stream facade and stream testkit.

use std::io::Write;
use std::time::Duration;

use rakka::prelude::{actor_fn, ActorAction, ActorContext, ActorSystem};
use rakka_process::{ExecutableAllowlist, ManagedProcess, ProcessSpec, ProcessStdio};
use rakka_stream::{AckProtocol, ActorSinkMessage, Sink, Source, StreamRunError};
use rakka_testkit::StreamTestKit;
use tokio::sync::mpsc;

const CHILD_FLAG: &str = "--stream-child";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == CHILD_FLAG) {
        run_child_process()?;
        return Ok(());
    }

    let finite_values = run_finite_operators().await?;
    let acked_items = run_actor_sink_with_ack().await?;
    let process_output = run_process_stdout_source().await?;
    let probed_items = run_stream_testkit_probe().await?;

    println!("Finite stream operators produced {finite_values:?}.");
    println!("Acked actor sink delivered {acked_items:?}.");
    println!("Process stdout facade source read {process_output:?}.");
    println!("Stream testkit probe collected {probed_items:?}.");

    Ok(())
}

async fn run_finite_operators() -> Result<Vec<u64>, StreamRunError<u64>> {
    Source::from_iter([1_u64, 2, 3, 4, 5])
        .map(|item| item * 2)
        .filter(|item| *item > 4)
        .take(2)
        .run_collect()
        .await
}

async fn run_actor_sink_with_ack() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let system = ActorSystem::new("stream-example-ack");
    let (events, mut receiver) = mpsc::unbounded_channel();
    let actor = system.spawn(
        "acked-sink",
        actor_fn(
            move |_ctx: &mut ActorContext<ActorSinkMessage<String, &'static str>>, msg| {
                match msg {
                    ActorSinkMessage::Init { reply_to } => {
                        let _ignored = events.send("init".to_owned());
                        let _ignored = reply_to.reply("ack");
                    }
                    ActorSinkMessage::Element { item, reply_to } => {
                        let _ignored = events.send(item);
                        let _ignored = reply_to.reply("ack");
                    }
                    ActorSinkMessage::Complete => {
                        let _ignored = events.send("complete".to_owned());
                    }
                    ActorSinkMessage::Failure { error } => {
                        let _ignored = events.send(format!("failure:{}", error.code()));
                    }
                    ActorSinkMessage::Cancelled { reason } => {
                        let _ignored = events.send(format!("cancelled:{reason}"));
                    }
                }
                Ok(ActorAction::Continue)
            },
        ),
    )?;

    let delivered = Source::from_iter(["apple".to_owned(), "banana".to_owned()])
        .run_with(Sink::actor_ref_with_ack(
            actor,
            AckProtocol::new("ack").with_timeout(Duration::from_secs(1)),
        ))
        .await?;
    assert_eq!(delivered, 2);

    let mut observed = Vec::new();
    while let Some(event) = receiver.recv().await {
        let complete = event == "complete";
        observed.push(event);
        if complete {
            break;
        }
    }

    system.terminate().await?;
    Ok(observed)
}

async fn run_process_stdout_source() -> Result<String, Box<dyn std::error::Error>> {
    let executable = std::env::current_exe()?;
    let allowlist = ExecutableAllowlist::from_exact_paths([executable.clone()]);
    let spec = ProcessSpec::new(executable)
        .arg(CHILD_FLAG)
        .stdout(ProcessStdio::Piped)
        .shutdown_timeout(Duration::from_secs(1));
    let mut process = ManagedProcess::spawn(spec, &allowlist)?;

    let (stdout, pump) =
        Source::process_stdout(&mut process, rakka_stream::ProcessOutputConfig::default())?;
    let chunks = stdout.run_collect().await?;
    let bytes_read = pump
        .expect("process stdout source should return a pump")
        .await??;
    let exit = process.wait().await?;
    assert!(exit.success(), "child process should exit successfully");

    let output = String::from_utf8(chunks.concat())?;
    assert_eq!(bytes_read, output.len());
    Ok(output.trim().to_owned())
}

async fn run_stream_testkit_probe() -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let (source, probe) = StreamTestKit::source_probe::<String>()?;
    let run = tokio::spawn(async move { source.run_collect().await });

    probe.send_next("probe-one".to_owned()).await?;
    probe.send_next("probe-two".to_owned()).await?;
    probe.send_complete()?;

    Ok(run.await??)
}

fn run_child_process() -> Result<(), Box<dyn std::error::Error>> {
    let mut stdout = std::io::stdout();
    writeln!(stdout, "child-stream-output")?;
    stdout.flush()?;
    Ok(())
}
