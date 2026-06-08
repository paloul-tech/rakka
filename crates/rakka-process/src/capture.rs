//! Bounded child-process output capture helpers.

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinHandle;

use crate::{ProcessError, ProcessResult};

pub(crate) type CaptureTask = JoinHandle<ProcessResult<Vec<u8>>>;

pub(crate) fn spawn_limited_reader<R>(
    stream: &'static str,
    mut reader: R,
    limit: usize,
) -> CaptureTask
where
    R: AsyncRead + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        let mut output = Vec::new();
        let mut buffer = [0_u8; 8192];

        loop {
            let read = reader
                .read(&mut buffer)
                .await
                .map_err(|error| ProcessError::StdioRead {
                    stream: stream.to_string(),
                    message: error.to_string(),
                })?;
            if read == 0 {
                return Ok(output);
            }
            if output.len().saturating_add(read) > limit {
                return Err(ProcessError::OutputLimitExceeded {
                    stream: stream.to_string(),
                    limit,
                });
            }
            output.extend_from_slice(&buffer[..read]);
        }
    })
}

pub(crate) async fn join_limited_reader(
    stream: &'static str,
    task: CaptureTask,
) -> ProcessResult<Vec<u8>> {
    task.await.map_err(|error| ProcessError::StdioRead {
        stream: stream.to_string(),
        message: error.to_string(),
    })?
}
