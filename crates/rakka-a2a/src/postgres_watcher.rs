//! PostgreSQL task-event watcher using bounded interval polling.
//!
//! Per the crate's replay design (Phase 7 DN-2), this watcher polls the
//! durable per-task high watermark (`MAX(sequence)` over
//! `rakka_a2a_task_events`) on a bounded interval and signals only that new
//! events may exist; subscribers then read durable events through the
//! projection store. `LISTEN/NOTIFY` support can be added later behind the
//! same [`A2ATaskEventWatcher`] trait without changing stream code.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_postgres::Client;

use crate::projection::{
    A2ATaskEventSignal, A2ATaskEventSignalOutcome, A2ATaskEventSignalSource, A2ATaskEventWatcher,
};
use crate::task::TaskProjectionResult;

/// Default watermark poll interval.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Consecutive poll failures tolerated before the signal reports itself lost.
const MAX_POLL_FAILURES: usize = 3;

/// Polling watcher over the shared `rakka_a2a_task_events` table.
#[derive(Clone)]
pub struct PostgresA2ATaskEventWatcher {
    client: Arc<Client>,
    poll_interval: Duration,
}

impl std::fmt::Debug for PostgresA2ATaskEventWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresA2ATaskEventWatcher")
            .field("poll_interval", &self.poll_interval)
            .finish_non_exhaustive()
    }
}

impl PostgresA2ATaskEventWatcher {
    /// Creates a watcher sharing the store's client.
    #[must_use]
    pub fn from_shared_client(client: Arc<Client>) -> Self {
        Self {
            client,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Overrides the watermark poll interval.
    #[must_use]
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }
}

async fn high_watermark(
    client: &Client,
    tenant: &str,
    task_id: &str,
) -> Result<u64, tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT COALESCE(MAX(sequence), 0) AS high FROM rakka_a2a_task_events \
             WHERE tenant = $1 AND task_id = $2",
            &[&tenant, &task_id],
        )
        .await?;
    Ok(u64::try_from(row.get::<_, i64>("high")).unwrap_or(0))
}

#[async_trait]
impl A2ATaskEventWatcher for PostgresA2ATaskEventWatcher {
    async fn watch(&self, tenant: &str, task_id: &str) -> TaskProjectionResult<A2ATaskEventSignal> {
        // Capture the baseline before returning: the subscriber replays the
        // durable log right after `watch`, so anything past this baseline is
        // guaranteed to produce a wake-up on a later poll.
        let baseline = high_watermark(&self.client, tenant, task_id)
            .await
            .unwrap_or(0);
        Ok(A2ATaskEventSignal::from_source(Box::new(
            PollSignalSource {
                client: Arc::clone(&self.client),
                tenant: tenant.to_string(),
                task_id: task_id.to_string(),
                poll_interval: self.poll_interval,
                baseline,
            },
        )))
    }
}

struct PollSignalSource {
    client: Arc<Client>,
    tenant: String,
    task_id: String,
    poll_interval: Duration,
    baseline: u64,
}

#[async_trait]
impl A2ATaskEventSignalSource for PollSignalSource {
    async fn changed(&mut self) -> A2ATaskEventSignalOutcome {
        let mut failures = 0;
        loop {
            tokio::time::sleep(self.poll_interval).await;
            match high_watermark(&self.client, &self.tenant, &self.task_id).await {
                Ok(current) if current > self.baseline => {
                    self.baseline = current;
                    return A2ATaskEventSignalOutcome::Notified {
                        high_watermark_hint: current,
                    };
                }
                Ok(_) => {
                    failures = 0;
                }
                Err(_) => {
                    failures += 1;
                    if failures >= MAX_POLL_FAILURES {
                        return A2ATaskEventSignalOutcome::Lost;
                    }
                }
            }
        }
    }
}
