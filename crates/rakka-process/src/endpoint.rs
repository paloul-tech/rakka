//! Local endpoint readiness foundations for socket and local gRPC process modes.

use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;
use std::time::Duration;

use tokio::net::TcpStream;

use crate::{
    ExecutableAllowlist, ManagedProcess, ProcessError, ProcessResult, ProcessShutdown, ProcessSpec,
};

const DEFAULT_ENDPOINT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_ENDPOINT_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Local endpoint owned by a child process.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LocalEndpoint {
    /// TCP endpoint bound on the local node.
    Tcp {
        /// Hostname or IP address.
        host: String,
        /// TCP port.
        port: u16,
    },
    /// Unix-domain socket endpoint bound on the local node.
    #[cfg(unix)]
    Unix {
        /// Unix-domain socket path.
        path: PathBuf,
    },
}

impl LocalEndpoint {
    /// Creates a TCP endpoint.
    #[must_use]
    pub fn tcp(host: impl Into<String>, port: u16) -> Self {
        Self::Tcp {
            host: host.into(),
            port,
        }
    }

    /// Creates a Unix-domain socket endpoint.
    #[cfg(unix)]
    #[must_use]
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self::Unix { path: path.into() }
    }

    async fn connect_once(&self) -> std::io::Result<()> {
        match self {
            Self::Tcp { host, port } => {
                let stream = TcpStream::connect((host.as_str(), *port)).await?;
                drop(stream);
                Ok(())
            }
            #[cfg(unix)]
            Self::Unix { path } => {
                let stream = tokio::net::UnixStream::connect(path).await?;
                drop(stream);
                Ok(())
            }
        }
    }
}

impl Display for LocalEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp { host, port } => write!(f, "tcp://{host}:{port}"),
            #[cfg(unix)]
            Self::Unix { path } => write!(f, "unix://{}", path.display()),
        }
    }
}

/// Configuration for waiting on local endpoint readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndpointReadinessConfig {
    timeout: Duration,
    poll_interval: Duration,
}

impl EndpointReadinessConfig {
    /// Creates default endpoint readiness configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            timeout: DEFAULT_ENDPOINT_TIMEOUT,
            poll_interval: DEFAULT_ENDPOINT_POLL_INTERVAL,
        }
    }

    /// Sets endpoint readiness timeout.
    #[must_use]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets endpoint readiness polling interval.
    #[must_use]
    pub const fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Endpoint readiness timeout.
    #[must_use]
    pub const fn timeout_duration(&self) -> Duration {
        self.timeout
    }

    /// Endpoint readiness polling interval.
    #[must_use]
    pub const fn poll_interval_duration(&self) -> Duration {
        self.poll_interval
    }
}

impl Default for EndpointReadinessConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Successful endpoint readiness observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointReady {
    endpoint: LocalEndpoint,
    attempts: usize,
}

impl EndpointReady {
    /// Creates endpoint readiness metadata.
    #[must_use]
    pub const fn new(endpoint: LocalEndpoint, attempts: usize) -> Self {
        Self { endpoint, attempts }
    }

    /// Endpoint that became reachable.
    #[must_use]
    pub const fn endpoint(&self) -> &LocalEndpoint {
        &self.endpoint
    }

    /// Number of connection attempts.
    #[must_use]
    pub const fn attempts(&self) -> usize {
        self.attempts
    }
}

/// Waits until a local endpoint accepts a connection.
pub async fn wait_for_local_endpoint(
    endpoint: LocalEndpoint,
    config: EndpointReadinessConfig,
) -> ProcessResult<EndpointReady> {
    let deadline = tokio::time::Instant::now() + config.timeout;
    let mut attempts = 0;

    loop {
        attempts += 1;
        match endpoint.connect_once().await {
            Ok(()) => return Ok(EndpointReady::new(endpoint, attempts)),
            Err(error) => {
                let last_error = Some(error.to_string());

                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(ProcessError::EndpointTimeout {
                        endpoint: endpoint.to_string(),
                        timeout: config.timeout,
                        last_error,
                    });
                }
                tokio::time::sleep(config.poll_interval.min(deadline - now)).await;
            }
        }
    }
}

/// Configuration for child-owned socket process startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SocketProcessConfig {
    readiness: EndpointReadinessConfig,
}

impl SocketProcessConfig {
    /// Creates socket process configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            readiness: EndpointReadinessConfig::new(),
        }
    }

    /// Sets endpoint readiness configuration.
    #[must_use]
    pub const fn readiness(mut self, readiness: EndpointReadinessConfig) -> Self {
        self.readiness = readiness;
        self
    }

    /// Endpoint readiness configuration.
    #[must_use]
    pub const fn readiness_config(&self) -> EndpointReadinessConfig {
        self.readiness
    }
}

/// Running child process with a ready local socket endpoint.
#[derive(Debug)]
pub struct SocketProcess {
    endpoint: LocalEndpoint,
    ready: EndpointReady,
    process: ManagedProcess,
}

impl SocketProcess {
    /// Creates a running socket process.
    #[must_use]
    pub const fn new(
        endpoint: LocalEndpoint,
        ready: EndpointReady,
        process: ManagedProcess,
    ) -> Self {
        Self {
            endpoint,
            ready,
            process,
        }
    }

    /// Ready local endpoint.
    #[must_use]
    pub const fn endpoint(&self) -> &LocalEndpoint {
        &self.endpoint
    }

    /// Endpoint readiness observation.
    #[must_use]
    pub const fn ready(&self) -> &EndpointReady {
        &self.ready
    }

    /// Owned managed process.
    #[must_use]
    pub const fn process(&self) -> &ManagedProcess {
        &self.process
    }

    /// Mutable owned managed process.
    #[must_use]
    pub const fn process_mut(&mut self) -> &mut ManagedProcess {
        &mut self.process
    }

    /// Shuts down the socket process.
    pub async fn shutdown(&mut self) -> ProcessResult<ProcessShutdown> {
        self.process.shutdown().await
    }
}

/// Starts a child process and waits for its local socket endpoint.
pub async fn start_socket_process(
    spec: ProcessSpec,
    allowlist: &ExecutableAllowlist,
    endpoint: LocalEndpoint,
    config: SocketProcessConfig,
) -> ProcessResult<SocketProcess> {
    let mut process = ManagedProcess::spawn(spec, allowlist)?;
    let ready = wait_for_process_endpoint(&mut process, endpoint.clone(), config.readiness).await?;
    Ok(SocketProcess::new(endpoint, ready, process))
}

/// Local gRPC endpoint metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalGrpcEndpoint {
    endpoint: LocalEndpoint,
    service_name: Option<String>,
}

impl LocalGrpcEndpoint {
    /// Creates a local gRPC endpoint from a local socket endpoint.
    #[must_use]
    pub const fn new(endpoint: LocalEndpoint) -> Self {
        Self {
            endpoint,
            service_name: None,
        }
    }

    /// Creates a local gRPC endpoint with service metadata.
    #[must_use]
    pub fn with_service_name(mut self, service_name: impl Into<String>) -> Self {
        self.service_name = Some(service_name.into());
        self
    }

    /// Creates a TCP local gRPC endpoint.
    #[must_use]
    pub fn tcp(host: impl Into<String>, port: u16) -> Self {
        Self::new(LocalEndpoint::tcp(host, port))
    }

    /// Creates a Unix-domain local gRPC endpoint.
    #[cfg(unix)]
    #[must_use]
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self::new(LocalEndpoint::unix(path))
    }

    /// Underlying local socket endpoint.
    #[must_use]
    pub const fn endpoint(&self) -> &LocalEndpoint {
        &self.endpoint
    }

    /// Optional gRPC service name metadata.
    #[must_use]
    pub fn service_name(&self) -> Option<&str> {
        self.service_name.as_deref()
    }
}

/// Configuration for local gRPC process startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LocalGrpcProcessConfig {
    readiness: EndpointReadinessConfig,
}

impl LocalGrpcProcessConfig {
    /// Creates local gRPC process configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            readiness: EndpointReadinessConfig::new(),
        }
    }

    /// Sets endpoint readiness configuration.
    #[must_use]
    pub const fn readiness(mut self, readiness: EndpointReadinessConfig) -> Self {
        self.readiness = readiness;
        self
    }

    /// Endpoint readiness configuration.
    #[must_use]
    pub const fn readiness_config(&self) -> EndpointReadinessConfig {
        self.readiness
    }
}

/// Running child process with a ready local gRPC endpoint.
#[derive(Debug)]
pub struct LocalGrpcProcess {
    endpoint: LocalGrpcEndpoint,
    socket_process: SocketProcess,
}

impl LocalGrpcProcess {
    /// Creates a local gRPC process.
    #[must_use]
    pub const fn new(endpoint: LocalGrpcEndpoint, socket_process: SocketProcess) -> Self {
        Self {
            endpoint,
            socket_process,
        }
    }

    /// Local gRPC endpoint metadata.
    #[must_use]
    pub const fn endpoint(&self) -> &LocalGrpcEndpoint {
        &self.endpoint
    }

    /// Underlying ready socket process.
    #[must_use]
    pub const fn socket_process(&self) -> &SocketProcess {
        &self.socket_process
    }

    /// Mutable underlying ready socket process.
    #[must_use]
    pub const fn socket_process_mut(&mut self) -> &mut SocketProcess {
        &mut self.socket_process
    }

    /// Shuts down the local gRPC process.
    pub async fn shutdown(&mut self) -> ProcessResult<ProcessShutdown> {
        self.socket_process.shutdown().await
    }
}

/// Starts a child process and waits for its local gRPC endpoint.
pub async fn start_local_grpc_process(
    spec: ProcessSpec,
    allowlist: &ExecutableAllowlist,
    endpoint: LocalGrpcEndpoint,
    config: LocalGrpcProcessConfig,
) -> ProcessResult<LocalGrpcProcess> {
    let socket_process = start_socket_process(
        spec,
        allowlist,
        endpoint.endpoint().clone(),
        SocketProcessConfig::new().readiness(config.readiness),
    )
    .await?;
    Ok(LocalGrpcProcess::new(endpoint, socket_process))
}

async fn wait_for_process_endpoint(
    process: &mut ManagedProcess,
    endpoint: LocalEndpoint,
    readiness: EndpointReadinessConfig,
) -> ProcessResult<EndpointReady> {
    let deadline = tokio::time::Instant::now() + readiness.timeout;
    let mut attempts = 0;

    loop {
        if let Some(exit) = process.try_wait()? {
            return Err(ProcessError::ExitedDuringStartup {
                code: exit.code(),
                signal: exit.signal(),
            });
        }

        attempts += 1;
        match endpoint.connect_once().await {
            Ok(()) => return Ok(EndpointReady::new(endpoint, attempts)),
            Err(error) => {
                let last_error = Some(error.to_string());

                let now = tokio::time::Instant::now();
                if now >= deadline {
                    let _shutdown = process.shutdown().await;
                    return Err(ProcessError::EndpointTimeout {
                        endpoint: endpoint.to_string(),
                        timeout: readiness.timeout,
                        last_error,
                    });
                }
                tokio::time::sleep(readiness.poll_interval.min(deadline - now)).await;
            }
        }
    }
}
