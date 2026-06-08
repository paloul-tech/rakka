//! Standard-IO process request/reply protocols.

use std::collections::{BTreeMap, VecDeque};
use std::marker::PhantomData;
use std::time::Duration;

use rakka_core::{
    actor_future, Actor, ActorAction, ActorContext, ActorFuture, ActorRef, ActorSystem,
    RakkaResult, ReplyTo,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStderr, ChildStdout};

use crate::{
    ExecutableAllowlist, ManagedProcess, ProcessError, ProcessExit, ProcessResult, ProcessSpec,
};

const DEFAULT_PENDING_CAPACITY: usize = 128;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_STDERR_CAPACITY: usize = 128;
const DEFAULT_SUPERVISION_INTERVAL: Duration = Duration::from_millis(50);
const RECENT_REQUEST_ID_LIMIT: usize = 256;

/// Codec for one newline-framed stdio request/reply protocol.
pub trait StdioCodec: Send + Sync + 'static {
    /// Typed request accepted by the protocol.
    type Request: Send + 'static;
    /// Typed response returned by the protocol.
    type Response: Send + 'static;

    /// Encodes a request into bytes written to child stdin.
    fn encode(&self, request_id: &str, request: Self::Request) -> ProcessResult<Vec<u8>>;

    /// Decodes one newline-delimited stdout frame.
    fn decode(&self, frame: &[u8]) -> ProcessResult<StdioReply<Self::Response>>;
}

/// Decoded stdio reply with request correlation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StdioReply<R> {
    request_id: String,
    response: R,
}

impl<R> StdioReply<R> {
    /// Creates a decoded stdio reply.
    #[must_use]
    pub fn new(request_id: impl Into<String>, response: R) -> Self {
        Self {
            request_id: request_id.into(),
            response,
        }
    }

    /// Reply request id.
    #[must_use]
    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Reply payload.
    #[must_use]
    pub const fn response(&self) -> &R {
        &self.response
    }

    fn into_parts(self) -> (String, R) {
        (self.request_id, self.response)
    }
}

/// Raw newline-framed stdio codec.
///
/// Frames are encoded as `<request-id> <payload>\n`. Payloads are raw bytes but
/// must not contain newlines because this v1 adapter is line framed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RawLineStdioCodec;

impl RawLineStdioCodec {
    /// Creates a raw line stdio codec.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl StdioCodec for RawLineStdioCodec {
    type Request = Vec<u8>;
    type Response = Vec<u8>;

    fn encode(&self, request_id: &str, request: Self::Request) -> ProcessResult<Vec<u8>> {
        validate_raw_request_id(request_id)?;
        if request.iter().any(|byte| *byte == b'\n' || *byte == b'\r') {
            return Err(ProcessError::ProtocolEncode {
                message: "raw stdio payloads must not contain newline bytes".to_string(),
            });
        }

        let mut frame = Vec::with_capacity(request_id.len() + 1 + request.len() + 1);
        frame.extend_from_slice(request_id.as_bytes());
        frame.push(b' ');
        frame.extend_from_slice(&request);
        frame.push(b'\n');
        Ok(frame)
    }

    fn decode(&self, frame: &[u8]) -> ProcessResult<StdioReply<Self::Response>> {
        let frame = trim_line_ending(frame);
        let Some(index) = frame.iter().position(|byte| *byte == b' ') else {
            return Err(ProcessError::MalformedStdout {
                message: "raw stdio reply is missing request id separator".to_string(),
            });
        };
        let request_id = std::str::from_utf8(&frame[..index]).map_err(|error| {
            ProcessError::MalformedStdout {
                message: format!("raw stdio request id is not utf-8: {error}"),
            }
        })?;
        validate_raw_request_id(request_id).map_err(|error| ProcessError::MalformedStdout {
            message: error.to_string(),
        })?;
        Ok(StdioReply::new(request_id, frame[index + 1..].to_vec()))
    }
}

/// Newline-delimited JSON frame used by `LineJsonCodec`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineJsonFrame<T> {
    /// Request or reply id.
    pub id: String,
    /// Frame payload.
    pub payload: T,
}

/// Newline-delimited JSON stdio codec.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LineJsonCodec<Req, Resp> {
    _message: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> LineJsonCodec<Req, Resp> {
    /// Creates a line-json stdio codec.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            _message: PhantomData,
        }
    }
}

impl<Req, Resp> StdioCodec for LineJsonCodec<Req, Resp>
where
    Req: Serialize + Send + 'static,
    Resp: DeserializeOwned + Send + 'static,
{
    type Request = Req;
    type Response = Resp;

    fn encode(&self, request_id: &str, request: Self::Request) -> ProcessResult<Vec<u8>> {
        validate_json_request_id(request_id)?;
        let frame = LineJsonFrame {
            id: request_id.to_string(),
            payload: request,
        };
        let mut bytes =
            serde_json::to_vec(&frame).map_err(|error| ProcessError::ProtocolEncode {
                message: error.to_string(),
            })?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    fn decode(&self, frame: &[u8]) -> ProcessResult<StdioReply<Self::Response>> {
        let frame: LineJsonFrame<Resp> =
            serde_json::from_slice(trim_line_ending(frame)).map_err(|error| {
                ProcessError::MalformedStdout {
                    message: error.to_string(),
                }
            })?;
        validate_json_request_id(&frame.id).map_err(|error| ProcessError::MalformedStdout {
            message: error.to_string(),
        })?;
        Ok(StdioReply::new(frame.id, frame.payload))
    }
}

/// Configuration for a stdio protocol actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StdioProtocolConfig {
    pending_capacity: usize,
    default_request_timeout: Duration,
    stderr_capacity: usize,
    supervision_interval: Duration,
}

impl StdioProtocolConfig {
    /// Creates default stdio protocol configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            pending_capacity: DEFAULT_PENDING_CAPACITY,
            default_request_timeout: DEFAULT_REQUEST_TIMEOUT,
            stderr_capacity: DEFAULT_STDERR_CAPACITY,
            supervision_interval: DEFAULT_SUPERVISION_INTERVAL,
        }
    }

    /// Sets bounded pending request capacity.
    #[must_use]
    pub const fn pending_capacity(mut self, pending_capacity: usize) -> Self {
        self.pending_capacity = pending_capacity;
        self
    }

    /// Sets the default request timeout.
    #[must_use]
    pub const fn default_request_timeout(mut self, default_request_timeout: Duration) -> Self {
        self.default_request_timeout = default_request_timeout;
        self
    }

    /// Sets captured stderr line capacity.
    #[must_use]
    pub const fn stderr_capacity(mut self, stderr_capacity: usize) -> Self {
        self.stderr_capacity = stderr_capacity;
        self
    }

    /// Sets child process supervision interval.
    #[must_use]
    pub const fn supervision_interval(mut self, supervision_interval: Duration) -> Self {
        self.supervision_interval = supervision_interval;
        self
    }

    /// Bounded pending request capacity.
    #[must_use]
    pub const fn pending_capacity_value(&self) -> usize {
        self.pending_capacity
    }

    /// Default request timeout.
    #[must_use]
    pub const fn default_request_timeout_duration(&self) -> Duration {
        self.default_request_timeout
    }
}

impl Default for StdioProtocolConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Current stdio protocol actor status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StdioStatus {
    pending_count: usize,
    closed: bool,
    stderr_count: usize,
    last_error: Option<ProcessError>,
    last_exit: Option<ProcessExit>,
}

impl StdioStatus {
    /// Creates a status snapshot.
    #[must_use]
    pub const fn new(
        pending_count: usize,
        closed: bool,
        stderr_count: usize,
        last_error: Option<ProcessError>,
        last_exit: Option<ProcessExit>,
    ) -> Self {
        Self {
            pending_count,
            closed,
            stderr_count,
            last_error,
            last_exit,
        }
    }

    /// Number of currently pending requests.
    #[must_use]
    pub const fn pending_count(&self) -> usize {
        self.pending_count
    }

    /// Returns true once the protocol is closed.
    #[must_use]
    pub const fn closed(&self) -> bool {
        self.closed
    }

    /// Number of captured stderr lines retained.
    #[must_use]
    pub const fn stderr_count(&self) -> usize {
        self.stderr_count
    }

    /// Last protocol error, when available.
    #[must_use]
    pub const fn last_error(&self) -> Option<&ProcessError> {
        self.last_error.as_ref()
    }

    /// Last process exit, when available.
    #[must_use]
    pub const fn last_exit(&self) -> Option<&ProcessExit> {
        self.last_exit.as_ref()
    }
}

/// Stdio protocol actor command.
pub enum StdioCommand<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    /// Sends a request using the configured default timeout.
    Request {
        /// Request payload.
        request: Req,
        /// Reply channel for the process response or failure.
        reply_to: ReplyTo<ProcessResult<Resp>>,
    },
    /// Sends a request using an explicit timeout.
    RequestWithTimeout {
        /// Request payload.
        request: Req,
        /// Request timeout.
        timeout: Duration,
        /// Reply channel for the process response or failure.
        reply_to: ReplyTo<ProcessResult<Resp>>,
    },
    /// Returns protocol status.
    Status {
        /// Reply channel for status.
        reply_to: ReplyTo<StdioStatus>,
    },
    /// Returns captured stderr lines.
    Stderr {
        /// Reply channel for retained stderr lines.
        reply_to: ReplyTo<Vec<String>>,
    },
    #[doc(hidden)]
    StdoutLine {
        #[doc(hidden)]
        line: Vec<u8>,
    },
    #[doc(hidden)]
    StdoutClosed,
    #[doc(hidden)]
    StdoutReadFailed {
        #[doc(hidden)]
        message: String,
    },
    #[doc(hidden)]
    StderrLine {
        #[doc(hidden)]
        line: String,
    },
    #[doc(hidden)]
    StderrReadFailed {
        #[doc(hidden)]
        message: String,
    },
    #[doc(hidden)]
    RequestTimedOut {
        #[doc(hidden)]
        request_id: String,
    },
    #[doc(hidden)]
    SupervisionTick {
        #[doc(hidden)]
        token: u64,
    },
}

struct Pending<Resp>
where
    Resp: Send + 'static,
{
    timeout: Duration,
    reply_to: ReplyTo<ProcessResult<Resp>>,
}

/// Actor that owns a child process and speaks a stdin/stdout protocol.
pub struct StdioActor<C>
where
    C: StdioCodec,
{
    spec: ProcessSpec,
    allowlist: ExecutableAllowlist,
    codec: C,
    config: StdioProtocolConfig,
    process: Option<ManagedProcess>,
    stdin: Option<tokio::process::ChildStdin>,
    pending: BTreeMap<String, Pending<C::Response>>,
    stderr: VecDeque<String>,
    recent_request_ids: VecDeque<String>,
    next_request_id: u64,
    closed: bool,
    last_error: Option<ProcessError>,
    last_exit: Option<ProcessExit>,
    tick_token: u64,
}

impl<C> StdioActor<C>
where
    C: StdioCodec,
{
    /// Creates a stdio protocol actor.
    #[must_use]
    pub fn new(
        spec: ProcessSpec,
        allowlist: ExecutableAllowlist,
        codec: C,
        config: StdioProtocolConfig,
    ) -> Self {
        Self {
            spec,
            allowlist,
            codec,
            config,
            process: None,
            stdin: None,
            pending: BTreeMap::new(),
            stderr: VecDeque::new(),
            recent_request_ids: VecDeque::new(),
            next_request_id: 0,
            closed: false,
            last_error: None,
            last_exit: None,
            tick_token: 0,
        }
    }

    /// Current protocol status.
    #[must_use]
    pub fn status(&self) -> StdioStatus {
        StdioStatus::new(
            self.pending.len(),
            self.closed,
            self.stderr.len(),
            self.last_error.clone(),
            self.last_exit.clone(),
        )
    }

    async fn start_stdio(&mut self, ctx: &ActorContext<StdioCommand<C::Request, C::Response>>) {
        match self.start_stdio_inner(ctx).await {
            Ok(()) => self.schedule_supervision_tick(ctx),
            Err(error) => {
                self.closed = true;
                self.last_error = Some(error);
            }
        }
    }

    async fn start_stdio_inner(
        &mut self,
        ctx: &ActorContext<StdioCommand<C::Request, C::Response>>,
    ) -> ProcessResult<()> {
        let mut process = ManagedProcess::spawn(self.spec.clone(), &self.allowlist)?;
        let stdin = process
            .take_stdin()
            .ok_or_else(|| ProcessError::MissingPipe {
                stream: "stdin".to_string(),
            })?;
        let stdout = process
            .take_stdout()
            .ok_or_else(|| ProcessError::MissingPipe {
                stream: "stdout".to_string(),
            })?;
        let stderr = process.take_stderr();

        self.spawn_stdout_reader(ctx, stdout);
        if let Some(stderr) = stderr {
            self.spawn_stderr_reader(ctx, stderr);
        }

        self.stdin = Some(stdin);
        self.process = Some(process);
        Ok(())
    }

    async fn request(
        &mut self,
        ctx: &ActorContext<StdioCommand<C::Request, C::Response>>,
        request: C::Request,
        timeout: Duration,
        reply_to: ReplyTo<ProcessResult<C::Response>>,
    ) {
        if self.closed {
            let error = self
                .last_error
                .clone()
                .unwrap_or_else(|| ProcessError::ProtocolClosed {
                    message: "stdio protocol is closed".to_string(),
                });
            let _sent = reply_to.reply(Err(error));
            return;
        }

        if self.pending.len() >= self.config.pending_capacity {
            let _sent = reply_to.reply(Err(ProcessError::PendingCapacity {
                capacity: self.config.pending_capacity,
            }));
            return;
        }

        let request_id = self.next_request_id();
        let frame = match self.codec.encode(&request_id, request) {
            Ok(frame) => frame,
            Err(error) => {
                let _sent = reply_to.reply(Err(error));
                return;
            }
        };

        self.pending
            .insert(request_id.clone(), Pending { timeout, reply_to });

        let write_result = if let Some(stdin) = &mut self.stdin {
            async {
                stdin.write_all(&frame).await?;
                stdin.flush().await
            }
            .await
            .map_err(|error| ProcessError::StdioWrite {
                message: error.to_string(),
            })
        } else {
            Err(ProcessError::StdinClosed)
        };

        if let Err(error) = write_result {
            if let Some(pending) = self.pending.remove(&request_id) {
                let _sent = pending.reply_to.reply(Err(error.clone()));
            }
            self.last_error = Some(error);
            return;
        }

        ctx.schedule_once(
            timeout,
            StdioCommand::RequestTimedOut {
                request_id: request_id.clone(),
            },
        );
    }

    async fn stdout_line(&mut self, line: Vec<u8>) {
        let decoded = match self.codec.decode(&line) {
            Ok(decoded) => decoded,
            Err(error) => {
                self.close_with_error(error).await;
                return;
            }
        };
        let (request_id, response) = decoded.into_parts();

        if let Some(pending) = self.pending.remove(&request_id) {
            self.remember_request_id(request_id);
            let _sent = pending.reply_to.reply(Ok(response));
            return;
        }

        let error = if self.recent_request_ids.iter().any(|id| id == &request_id) {
            ProcessError::DuplicateReply { request_id }
        } else {
            ProcessError::UnknownReply { request_id }
        };
        self.last_error = Some(error);
    }

    async fn stdout_closed(&mut self) {
        self.close_with_error(ProcessError::StdoutClosed).await;
    }

    async fn request_timeout(&mut self, request_id: String) {
        if let Some(pending) = self.pending.remove(&request_id) {
            self.remember_request_id(request_id.clone());
            let _sent = pending.reply_to.reply(Err(ProcessError::RequestTimeout {
                timeout: pending.timeout,
                request_id,
            }));
        }
    }

    async fn supervision_tick(
        &mut self,
        ctx: &ActorContext<StdioCommand<C::Request, C::Response>>,
        token: u64,
    ) {
        if token != self.tick_token || self.closed {
            return;
        }

        if let Some(process) = &mut self.process {
            match process.try_wait() {
                Ok(Some(exit)) => {
                    self.last_exit = Some(exit.clone());
                    self.process = None;
                    self.close_with_error(ProcessError::UnexpectedExit {
                        code: exit.code(),
                        signal: exit.signal(),
                    })
                    .await;
                }
                Ok(None) => self.schedule_supervision_tick(ctx),
                Err(error) => self.close_with_error(error).await,
            }
        }
    }

    async fn close_with_error(&mut self, error: ProcessError) {
        if self.closed {
            self.last_error = Some(error);
            return;
        }

        self.closed = true;
        self.last_error = Some(error.clone());
        self.stdin = None;
        if let Some(mut process) = self.process.take() {
            let _shutdown = process.shutdown().await;
        }
        self.fail_all(error);
    }

    async fn stop_protocol(&mut self) {
        self.closed = true;
        self.stdin = None;
        if let Some(mut process) = self.process.take() {
            let _shutdown = process.shutdown().await;
        }
        self.fail_all(ProcessError::ProtocolClosed {
            message: "stdio actor stopped".to_string(),
        });
    }

    fn fail_all(&mut self, error: ProcessError) {
        let pending = std::mem::take(&mut self.pending);
        for (request_id, pending) in pending {
            self.remember_request_id(request_id);
            let _sent = pending.reply_to.reply(Err(error.clone()));
        }
    }

    fn next_request_id(&mut self) -> String {
        self.next_request_id = self.next_request_id.wrapping_add(1);
        format!("stdio-{}", self.next_request_id)
    }

    fn remember_request_id(&mut self, request_id: String) {
        if self.recent_request_ids.len() == RECENT_REQUEST_ID_LIMIT {
            self.recent_request_ids.pop_front();
        }
        self.recent_request_ids.push_back(request_id);
    }

    fn push_stderr(&mut self, line: String) {
        if self.stderr.len() == self.config.stderr_capacity {
            self.stderr.pop_front();
        }
        self.stderr.push_back(line);
    }

    fn schedule_supervision_tick(
        &mut self,
        ctx: &ActorContext<StdioCommand<C::Request, C::Response>>,
    ) {
        self.tick_token = self.tick_token.wrapping_add(1);
        let token = self.tick_token;
        ctx.schedule_once(
            self.config.supervision_interval,
            StdioCommand::SupervisionTick { token },
        );
    }

    fn spawn_stdout_reader(
        &self,
        ctx: &ActorContext<StdioCommand<C::Request, C::Response>>,
        stdout: ChildStdout,
    ) {
        let myself = ctx.myself().clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = Vec::new();
                match reader.read_until(b'\n', &mut line).await {
                    Ok(0) => {
                        let _sent = myself.tell(StdioCommand::StdoutClosed);
                        break;
                    }
                    Ok(_read) => {
                        let _sent = myself.tell(StdioCommand::StdoutLine { line });
                    }
                    Err(error) => {
                        let _sent = myself.tell(StdioCommand::StdoutReadFailed {
                            message: error.to_string(),
                        });
                        break;
                    }
                }
            }
        });
    }

    fn spawn_stderr_reader(
        &self,
        ctx: &ActorContext<StdioCommand<C::Request, C::Response>>,
        stderr: ChildStderr,
    ) {
        let myself = ctx.myself().clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            loop {
                let mut line = Vec::new();
                match reader.read_until(b'\n', &mut line).await {
                    Ok(0) => break,
                    Ok(_read) => {
                        let line = String::from_utf8_lossy(trim_line_ending(&line)).to_string();
                        let _sent = myself.tell(StdioCommand::StderrLine { line });
                    }
                    Err(error) => {
                        let _sent = myself.tell(StdioCommand::StderrReadFailed {
                            message: error.to_string(),
                        });
                        break;
                    }
                }
            }
        });
    }
}

impl<C> Actor for StdioActor<C>
where
    C: StdioCodec,
{
    type Msg = StdioCommand<C::Request, C::Response>;

    fn started<'a>(&'a mut self, ctx: &'a mut ActorContext<Self::Msg>) -> ActorFuture<'a> {
        actor_future(async move {
            self.start_stdio(ctx).await;
            Ok(ActorAction::Continue)
        })
    }

    fn handle<'a>(
        &'a mut self,
        ctx: &'a mut ActorContext<Self::Msg>,
        msg: Self::Msg,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            match msg {
                StdioCommand::Request { request, reply_to } => {
                    self.request(ctx, request, self.config.default_request_timeout, reply_to)
                        .await;
                }
                StdioCommand::RequestWithTimeout {
                    request,
                    timeout,
                    reply_to,
                } => self.request(ctx, request, timeout, reply_to).await,
                StdioCommand::Status { reply_to } => {
                    let _sent = reply_to.reply(self.status());
                }
                StdioCommand::Stderr { reply_to } => {
                    let _sent = reply_to.reply(self.stderr.iter().cloned().collect());
                }
                StdioCommand::StdoutLine { line } => self.stdout_line(line).await,
                StdioCommand::StdoutClosed => self.stdout_closed().await,
                StdioCommand::StdoutReadFailed { message } => {
                    self.close_with_error(ProcessError::StdioRead {
                        stream: "stdout".to_string(),
                        message,
                    })
                    .await;
                }
                StdioCommand::StderrLine { line } => self.push_stderr(line),
                StdioCommand::StderrReadFailed { message } => {
                    self.last_error = Some(ProcessError::StdioRead {
                        stream: "stderr".to_string(),
                        message,
                    });
                }
                StdioCommand::RequestTimedOut { request_id } => {
                    self.request_timeout(request_id).await;
                }
                StdioCommand::SupervisionTick { token } => {
                    self.supervision_tick(ctx, token).await;
                }
            }

            Ok(ActorAction::Continue)
        })
    }

    fn stopped<'a>(
        &'a mut self,
        _ctx: &'a mut ActorContext<Self::Msg>,
        _reason: &'a rakka_core::TerminationReason,
    ) -> ActorFuture<'a> {
        actor_future(async move {
            self.stop_protocol().await;
            Ok(ActorAction::Continue)
        })
    }
}

/// Spawns a stdio protocol actor.
pub fn spawn_stdio_actor<C>(
    system: &ActorSystem,
    name: impl AsRef<str>,
    spec: ProcessSpec,
    allowlist: ExecutableAllowlist,
    codec: C,
    config: StdioProtocolConfig,
) -> RakkaResult<ActorRef<StdioCommand<C::Request, C::Response>>>
where
    C: StdioCodec,
{
    system.spawn_actor(name, StdioActor::new(spec, allowlist, codec, config))
}

fn validate_raw_request_id(request_id: &str) -> ProcessResult<()> {
    if request_id.is_empty() || request_id.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(ProcessError::ProtocolEncode {
            message: format!("invalid raw stdio request id: {request_id:?}"),
        });
    }
    Ok(())
}

fn validate_json_request_id(request_id: &str) -> ProcessResult<()> {
    if request_id.is_empty() || request_id.contains('\n') || request_id.contains('\r') {
        return Err(ProcessError::ProtocolEncode {
            message: format!("invalid line-json request id: {request_id:?}"),
        });
    }
    Ok(())
}

fn trim_line_ending(mut line: &[u8]) -> &[u8] {
    if line.ends_with(b"\n") {
        line = &line[..line.len() - 1];
    }
    if line.ends_with(b"\r") {
        line = &line[..line.len() - 1];
    }
    line
}
