//! Process pipe adapters for bounded byte streams.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use rakka_process::{ManagedProcess, ProcessError};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::process::ChildStdin;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

use crate::{
    bounded_channel, Sink, Source, StreamError, StreamSendError, StreamSink, StreamSource,
    StreamStatus, DEFAULT_BUFFER_CAPACITY,
};

/// Default byte chunk size used when pumping process output.
pub const DEFAULT_PROCESS_IO_CHUNK_SIZE: usize = 8192;

/// Result alias for process stream adapter operations.
pub type ProcessStreamResult<T> = Result<T, ProcessStreamError>;

/// Join handle returned by process output pump tasks.
pub type ProcessStreamPump = JoinHandle<ProcessStreamResult<usize>>;

/// Managed process stdin sink type.
pub type ManagedProcessStdinSink = ProcessInputSink<ChildStdin>;

/// Process pipe represented by a stream adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessIoStream {
    /// Child process stdin.
    Stdin,
    /// Child process stdout.
    Stdout,
    /// Child process stderr.
    Stderr,
}

impl ProcessIoStream {
    /// Stable stream name for errors and telemetry attributes.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stdin => "stdin",
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

impl Display for ProcessIoStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Owner of a process pipe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessIoOwner {
    /// Pipe is directly owned by a `ManagedProcess`.
    ManagedProcess,
    /// Pipe is owned by a protocol actor such as `StdioActor`.
    ProtocolActor,
}

impl ProcessIoOwner {
    /// Stable owner name for errors and telemetry attributes.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ManagedProcess => "managed-process",
            Self::ProtocolActor => "protocol-actor",
        }
    }
}

impl Display for ProcessIoOwner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Failure returned by process pipe stream adapters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessStreamError {
    /// A required managed process pipe was not available.
    MissingPipe {
        /// Missing process pipe.
        stream: ProcessIoStream,
    },
    /// Reading from process stdout or stderr failed.
    Read {
        /// Pipe that failed.
        stream: ProcessIoStream,
        /// Operating-system error message.
        message: String,
    },
    /// Writing to process stdin failed.
    Write {
        /// Pipe that failed.
        stream: ProcessIoStream,
        /// Operating-system error message.
        message: String,
    },
    /// Bounded stream lifecycle prevented adapter progress.
    Stream {
        /// Stream lifecycle failure.
        error: StreamError,
    },
    /// The requested pipe is owned by another process adapter boundary.
    UnsupportedOwner {
        /// Pipe requested by the caller.
        stream: ProcessIoStream,
        /// Current pipe owner.
        owner: ProcessIoOwner,
    },
    /// Output pump task failed before returning a process stream result.
    PumpJoin {
        /// Join failure detail.
        message: String,
    },
}

impl ProcessStreamError {
    /// Converts this adapter error to the nearest process crate error.
    #[must_use]
    pub fn into_process_error(self) -> ProcessError {
        match self {
            Self::MissingPipe { stream } => ProcessError::MissingPipe {
                stream: stream.as_str().to_string(),
            },
            Self::Read { stream, message } => ProcessError::StdioRead {
                stream: stream.as_str().to_string(),
                message,
            },
            Self::Write { message, .. } => ProcessError::StdioWrite { message },
            Self::Stream { error } => ProcessError::ProtocolClosed {
                message: error.to_string(),
            },
            Self::UnsupportedOwner { stream, owner } => ProcessError::ProtocolClosed {
                message: format!("{stream} pipe is owned by {owner}"),
            },
            Self::PumpJoin { message } => ProcessError::ProtocolClosed { message },
        }
    }
}

impl Display for ProcessStreamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPipe { stream } => write!(f, "process {stream} pipe was not available"),
            Self::Read { stream, message } => {
                write!(f, "failed to read process {stream}: {message}")
            }
            Self::Write { stream, message } => {
                write!(f, "failed to write process {stream}: {message}")
            }
            Self::Stream { error } => Display::fmt(error, f),
            Self::UnsupportedOwner { stream, owner } => {
                write!(f, "process {stream} pipe is owned by {owner}")
            }
            Self::PumpJoin { message } => write!(f, "process stream pump failed: {message}"),
        }
    }
}

impl Error for ProcessStreamError {}

/// Configuration for a process stdout or stderr byte stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessOutputConfig {
    capacity: usize,
    chunk_size: usize,
}

impl ProcessOutputConfig {
    /// Creates a process output config with the requested bounded capacity.
    #[must_use]
    pub const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            chunk_size: DEFAULT_PROCESS_IO_CHUNK_SIZE,
        }
    }

    /// Sets the maximum byte chunk size read from the process pipe.
    #[must_use]
    pub const fn chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Bounded stream capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Maximum byte chunk size read from the process pipe.
    #[must_use]
    pub const fn chunk_size_value(&self) -> usize {
        self.chunk_size
    }
}

impl Default for ProcessOutputConfig {
    fn default() -> Self {
        Self::new(DEFAULT_BUFFER_CAPACITY)
    }
}

/// Bounded source backed by a process stdout or stderr pump.
pub struct ProcessOutputStream {
    stream: ProcessIoStream,
    source: StreamSource<Vec<u8>>,
    pump: Option<ProcessStreamPump>,
}

impl ProcessOutputStream {
    /// Process pipe represented by this output stream.
    #[must_use]
    pub const fn stream(&self) -> ProcessIoStream {
        self.stream
    }

    /// Bounded source receiving process output chunks.
    #[must_use]
    pub const fn source(&self) -> &StreamSource<Vec<u8>> {
        &self.source
    }

    /// Current bounded source status.
    #[must_use]
    pub fn status(&self) -> StreamStatus {
        self.source.status()
    }

    /// Cancels the process output stream and drops the read pipe by aborting the pump.
    pub fn cancel(&mut self, reason: impl Into<String>) -> usize {
        if let Some(pump) = &self.pump {
            pump.abort();
        }
        self.source.cancel(reason)
    }

    /// Awaits the process output pump result.
    pub async fn join(&mut self) -> ProcessStreamResult<usize> {
        let Some(pump) = self.pump.take() else {
            return Ok(0);
        };

        pump.await.map_err(|error| ProcessStreamError::PumpJoin {
            message: error.to_string(),
        })?
    }

    /// Consumes this stream into its bounded source and pump task.
    #[must_use]
    pub fn into_parts(mut self) -> (StreamSource<Vec<u8>>, Option<ProcessStreamPump>) {
        (self.source, self.pump.take())
    }

    /// Consumes this output stream into a facade source and its pump task.
    ///
    /// The returned pump is the same handle exposed by `into_parts`, preserving
    /// direct access to read completion and read errors after migrating to the
    /// facade source.
    #[must_use]
    pub fn into_source(mut self) -> (Source<Vec<u8>>, Option<ProcessStreamPump>) {
        (self.source.into_source(), self.pump.take())
    }
}

impl fmt::Debug for ProcessOutputStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessOutputStream")
            .field("stream", &self.stream)
            .field("status", &self.status())
            .field("pump_running", &self.pump.is_some())
            .finish()
    }
}

/// Creates a process output stream from any async reader.
pub fn process_output_stream_from_reader<R>(
    reader: R,
    stream: ProcessIoStream,
    config: ProcessOutputConfig,
) -> ProcessStreamResult<ProcessOutputStream>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (sink, source) =
        bounded_channel(config.capacity()).map_err(|error| ProcessStreamError::Stream { error })?;
    let pump = spawn_output_pump(reader, sink, stream, config.chunk_size_value().max(1));
    Ok(ProcessOutputStream {
        stream,
        source,
        pump: Some(pump),
    })
}

/// Creates a bounded stdout stream by taking stdout from a managed process.
pub fn managed_process_stdout_stream(
    process: &mut ManagedProcess,
    config: ProcessOutputConfig,
) -> ProcessStreamResult<ProcessOutputStream> {
    let stdout = process
        .take_stdout()
        .ok_or(ProcessStreamError::MissingPipe {
            stream: ProcessIoStream::Stdout,
        })?;
    process_output_stream_from_reader(stdout, ProcessIoStream::Stdout, config)
}

/// Creates a bounded stderr stream by taking stderr from a managed process.
pub fn managed_process_stderr_stream(
    process: &mut ManagedProcess,
    config: ProcessOutputConfig,
) -> ProcessStreamResult<ProcessOutputStream> {
    let stderr = process
        .take_stderr()
        .ok_or(ProcessStreamError::MissingPipe {
            stream: ProcessIoStream::Stderr,
        })?;
    process_output_stream_from_reader(stderr, ProcessIoStream::Stderr, config)
}

impl Source<Vec<u8>> {
    /// Creates a facade source by taking stdout from a managed process.
    pub fn process_stdout(
        process: &mut ManagedProcess,
        config: ProcessOutputConfig,
    ) -> ProcessStreamResult<(Self, Option<ProcessStreamPump>)> {
        Ok(managed_process_stdout_stream(process, config)?.into_source())
    }

    /// Creates a facade source by taking stderr from a managed process.
    pub fn process_stderr(
        process: &mut ManagedProcess,
        config: ProcessOutputConfig,
    ) -> ProcessStreamResult<(Self, Option<ProcessStreamPump>)> {
        Ok(managed_process_stderr_stream(process, config)?.into_source())
    }
}

/// Creates an error for protocol-actor-owned process pipes.
pub fn protocol_actor_process_stream_unsupported(
    stream: ProcessIoStream,
) -> ProcessStreamResult<()> {
    Err(ProcessStreamError::UnsupportedOwner {
        stream,
        owner: ProcessIoOwner::ProtocolActor,
    })
}

fn spawn_output_pump<R>(
    mut reader: R,
    sink: StreamSink<Vec<u8>>,
    stream: ProcessIoStream,
    chunk_size: usize,
) -> ProcessStreamPump
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut total_bytes = 0usize;
        let mut buffer = vec![0u8; chunk_size];

        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => {
                    sink.drain()
                        .map_err(|error| ProcessStreamError::Stream { error })?;
                    return Ok(total_bytes);
                }
                Ok(read) => {
                    total_bytes = total_bytes.saturating_add(read);
                    let chunk = buffer[..read].to_vec();
                    sink.send(chunk).await.map_err(|error| {
                        let (error, _chunk) = error.into_parts();
                        ProcessStreamError::Stream { error }
                    })?;
                }
                Err(error) => {
                    let message = error.to_string();
                    sink.cancel(format!("process {stream} read failed: {message}"));
                    return Err(ProcessStreamError::Read { stream, message });
                }
            }
        }
    })
}

/// Bounded byte sink backed by a process stdin writer.
pub struct ProcessInputSink<W>
where
    W: AsyncWrite + Unpin,
{
    stream: ProcessIoStream,
    writer: Option<W>,
}

impl<W> ProcessInputSink<W>
where
    W: AsyncWrite + Unpin,
{
    /// Process pipe represented by this input sink.
    #[must_use]
    pub const fn stream(&self) -> ProcessIoStream {
        self.stream
    }

    /// Writes one byte chunk to the underlying process pipe.
    pub async fn write(&mut self, bytes: &[u8]) -> ProcessStreamResult<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or(ProcessStreamError::MissingPipe {
                stream: self.stream,
            })?;
        writer
            .write_all(bytes)
            .await
            .map_err(|error| ProcessStreamError::Write {
                stream: self.stream,
                message: error.to_string(),
            })?;
        writer
            .flush()
            .await
            .map_err(|error| ProcessStreamError::Write {
                stream: self.stream,
                message: error.to_string(),
            })
    }

    /// Pipes a bounded byte source into process stdin until normal completion.
    pub async fn drain_from(
        &mut self,
        source: &StreamSource<Vec<u8>>,
    ) -> ProcessStreamResult<usize> {
        let mut chunks = 0usize;
        loop {
            match source.next().await {
                Ok(Some(bytes)) => {
                    self.write(&bytes).await?;
                    chunks = chunks.saturating_add(1);
                }
                Ok(None) => {
                    self.close();
                    return Ok(chunks);
                }
                Err(error) => return Err(ProcessStreamError::Stream { error }),
            }
        }
    }

    /// Closes the underlying write pipe by dropping it.
    pub fn close(&mut self) {
        self.writer.take();
    }

    /// Cancels this input sink and closes the underlying write pipe.
    pub fn cancel(&mut self) {
        self.close();
    }
}

impl<W> ProcessInputSink<W>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    /// Consumes this process input adapter into a facade sink.
    ///
    /// The facade sink writes and flushes each received chunk before accepting
    /// the next one. Dropping or completing the sink drops the owned stdin pipe.
    #[must_use]
    pub fn into_sink(self) -> Sink<Vec<u8>, usize> {
        let input = Arc::new(AsyncMutex::new(self));

        Sink::from_async_consumer(move |bytes: Vec<u8>| {
            let input = Arc::clone(&input);
            async move {
                let mut input = input.lock().await;
                input
                    .write(&bytes)
                    .await
                    .map_err(|error| StreamSendError::new(stream_error_from_process(error), bytes))
            }
        })
    }
}

impl Sink<Vec<u8>, usize> {
    /// Creates a facade sink by taking stdin from a managed process.
    pub fn process_stdin(process: &mut ManagedProcess) -> ProcessStreamResult<Self> {
        Ok(managed_process_stdin_sink(process)?.into_sink())
    }
}

impl<W> fmt::Debug for ProcessInputSink<W>
where
    W: AsyncWrite + Unpin,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessInputSink")
            .field("stream", &self.stream)
            .field("open", &self.writer.is_some())
            .finish()
    }
}

/// Creates a process input sink from any async writer.
#[must_use]
pub fn process_input_sink_from_writer<W>(writer: W, stream: ProcessIoStream) -> ProcessInputSink<W>
where
    W: AsyncWrite + Unpin,
{
    ProcessInputSink {
        stream,
        writer: Some(writer),
    }
}

/// Creates a stdin sink by taking stdin from a managed process.
pub fn managed_process_stdin_sink(
    process: &mut ManagedProcess,
) -> ProcessStreamResult<ManagedProcessStdinSink> {
    let stdin = process
        .take_stdin()
        .ok_or(ProcessStreamError::MissingPipe {
            stream: ProcessIoStream::Stdin,
        })?;
    Ok(process_input_sink_from_writer(
        stdin,
        ProcessIoStream::Stdin,
    ))
}

fn stream_error_from_process(error: ProcessStreamError) -> StreamError {
    match error {
        ProcessStreamError::Stream { error } => error,
        error => StreamError::Operator {
            message: error.to_string(),
        },
    }
}
