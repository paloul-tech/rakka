//! Kubernetes pre-stop drain orchestration.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_cluster::MembershipState;
use rakka_core::ActorRef;
use rakka_process::{ProcessActorCommand, ProcessActorStatus, ProcessError};
use rakka_sharding::ClusterShardingRuntime;
use rakka_stream::{StreamError, StreamSink, StreamSource};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::KubernetesNodeHealth;

/// Future returned by a Kubernetes drain step.
pub type KubernetesDrainFuture =
    Pin<Box<dyn Future<Output = KubernetesDrainStepResult> + Send + 'static>>;

/// One named pre-stop drain hook.
pub trait KubernetesDrainStep: Send + Sync {
    /// Stable step name.
    fn name(&self) -> &str;

    /// Runs the drain step.
    fn run(&self) -> KubernetesDrainFuture;
}

/// Result returned by an individual drain step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KubernetesDrainStepResult {
    /// Step completed successfully.
    Completed {
        /// Human-readable detail.
        message: String,
    },
    /// Step failed before the drain deadline.
    Failed {
        /// Human-readable failure detail.
        message: String,
    },
}

impl KubernetesDrainStepResult {
    /// Creates a successful step result.
    #[must_use]
    pub fn completed(message: impl Into<String>) -> Self {
        Self::Completed {
            message: message.into(),
        }
    }

    /// Creates a failed step result.
    #[must_use]
    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
        }
    }
}

/// Stable drain step status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KubernetesDrainStepStatus {
    /// Step completed successfully.
    Completed,
    /// Step failed before the deadline.
    Failed,
    /// Step did not finish before the deadline.
    TimedOut,
}

/// Point-in-time report for one drain step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KubernetesDrainStepReport {
    name: String,
    status: KubernetesDrainStepStatus,
    message: String,
}

impl KubernetesDrainStepReport {
    /// Creates a drain step report.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        status: KubernetesDrainStepStatus,
        message: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            status,
            message: message.into(),
        }
    }

    /// Stable step name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Step status.
    #[must_use]
    pub const fn status(&self) -> KubernetesDrainStepStatus {
        self.status
    }

    /// Human-readable detail.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Overall drain result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KubernetesDrainOutcome {
    /// Every registered step completed.
    Complete,
    /// At least one step failed, but no step timed out.
    Partial,
    /// Drain deadline elapsed before all steps completed.
    TimedOut,
}

/// Full pre-stop drain report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KubernetesDrainReport {
    outcome: KubernetesDrainOutcome,
    steps: Vec<KubernetesDrainStepReport>,
}

impl KubernetesDrainReport {
    /// Creates a drain report.
    #[must_use]
    pub fn new(outcome: KubernetesDrainOutcome, steps: Vec<KubernetesDrainStepReport>) -> Self {
        Self { outcome, steps }
    }

    /// Overall drain outcome.
    #[must_use]
    pub const fn outcome(&self) -> KubernetesDrainOutcome {
        self.outcome
    }

    /// Reports for steps that ran or timed out.
    #[must_use]
    pub fn steps(&self) -> &[KubernetesDrainStepReport] {
        &self.steps
    }

    /// Returns true when every registered step completed.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self.outcome, KubernetesDrainOutcome::Complete)
    }
}

/// Pre-stop drain controller that marks readiness false and runs registered hooks.
pub struct KubernetesDrainController {
    health: KubernetesNodeHealth,
    steps: Vec<Arc<dyn KubernetesDrainStep>>,
}

impl KubernetesDrainController {
    /// Creates a drain controller for the provided health model.
    #[must_use]
    pub fn new(health: KubernetesNodeHealth) -> Self {
        Self {
            health,
            steps: Vec::new(),
        }
    }

    /// Shared health model updated by this controller.
    #[must_use]
    pub const fn health(&self) -> &KubernetesNodeHealth {
        &self.health
    }

    /// Registered drain step count.
    #[must_use]
    pub fn step_count(&self) -> usize {
        self.steps.len()
    }

    /// Adds a custom async drain step.
    pub fn add_step<F, Fut>(&mut self, name: impl Into<String>, run: F) -> &mut Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = KubernetesDrainStepResult> + Send + 'static,
    {
        self.steps.push(Arc::new(FnDrainStep {
            name: name.into(),
            run,
        }));
        self
    }

    /// Adds a step that marks the local node leaving in a cluster sharding runtime.
    pub fn add_sharding_runtime_leave(
        &mut self,
        name: impl Into<String>,
        runtime: Arc<Mutex<ClusterShardingRuntime>>,
        observed_at_millis: u64,
    ) -> &mut Self {
        self.add_step(name, move || {
            let runtime = Arc::clone(&runtime);
            async move {
                let mut runtime = runtime
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let local_node_id = runtime.membership().local_node_id().clone();
                if runtime
                    .membership()
                    .member(&local_node_id)
                    .is_some_and(|member| member.state() == MembershipState::Leaving)
                {
                    return KubernetesDrainStepResult::completed("local node already leaving");
                }

                match runtime.mark_leaving(&local_node_id, observed_at_millis) {
                    Ok(update) => KubernetesDrainStepResult::completed(format!(
                        "membership_events={}, handoffs={}, rebalances={}",
                        update.membership_events().len(),
                        update.handoffs().len(),
                        update.rebalances().len()
                    )),
                    Err(error) => KubernetesDrainStepResult::failed(error.to_string()),
                }
            }
        })
    }

    /// Adds a step that gracefully drains a Rakka stream sink.
    pub fn add_stream_sink<T>(&mut self, name: impl Into<String>, sink: StreamSink<T>) -> &mut Self
    where
        T: Send + 'static,
    {
        self.add_step(name, move || {
            let sink = sink.clone();
            async move { stream_drain_result(sink.drain(), "stream sink draining") }
        })
    }

    /// Adds a step that gracefully drains a Rakka stream source.
    pub fn add_stream_source<T>(
        &mut self,
        name: impl Into<String>,
        source: StreamSource<T>,
    ) -> &mut Self
    where
        T: Send + 'static,
    {
        self.add_step(name, move || {
            let source = source.clone();
            async move { stream_drain_result(source.drain(), "stream source draining") }
        })
    }

    /// Adds a step that sends `ProcessActorCommand::Stop` to a process actor.
    pub fn add_process_actor_stop(
        &mut self,
        name: impl Into<String>,
        actor: ActorRef<ProcessActorCommand>,
        timeout: Duration,
    ) -> &mut Self {
        self.add_step(name, move || {
            let actor = actor.clone();
            async move {
                match actor
                    .ask(|reply_to| ProcessActorCommand::Stop { reply_to }, timeout)
                    .await
                {
                    Ok(Ok(status)) => process_stop_completed(&status),
                    Ok(Err(ProcessError::NotRunning)) => {
                        KubernetesDrainStepResult::completed("process actor already stopped")
                    }
                    Ok(Err(error)) => KubernetesDrainStepResult::failed(error.to_string()),
                    Err(error) => KubernetesDrainStepResult::failed(error.to_string()),
                }
            }
        })
    }

    /// Runs pre-stop drain steps until all complete or the deadline elapses.
    pub async fn drain(&self, timeout: Duration) -> KubernetesDrainReport {
        self.health.begin_drain();
        let steps = self.steps.clone();
        let deadline = Instant::now() + timeout;
        let mut reports = Vec::new();
        let mut timed_out = false;

        for step in steps {
            let now = Instant::now();
            if now >= deadline {
                reports.push(KubernetesDrainStepReport::new(
                    step.name(),
                    KubernetesDrainStepStatus::TimedOut,
                    "drain deadline elapsed before step started",
                ));
                timed_out = true;
                break;
            }

            match tokio::time::timeout_at(deadline, step.run()).await {
                Ok(KubernetesDrainStepResult::Completed { message }) => {
                    reports.push(KubernetesDrainStepReport::new(
                        step.name(),
                        KubernetesDrainStepStatus::Completed,
                        message,
                    ));
                }
                Ok(KubernetesDrainStepResult::Failed { message }) => {
                    reports.push(KubernetesDrainStepReport::new(
                        step.name(),
                        KubernetesDrainStepStatus::Failed,
                        message,
                    ));
                }
                Err(_elapsed) => {
                    reports.push(KubernetesDrainStepReport::new(
                        step.name(),
                        KubernetesDrainStepStatus::TimedOut,
                        "drain step timed out",
                    ));
                    timed_out = true;
                    break;
                }
            }
        }

        let outcome = if timed_out {
            KubernetesDrainOutcome::TimedOut
        } else if reports
            .iter()
            .all(|report| report.status() == KubernetesDrainStepStatus::Completed)
        {
            KubernetesDrainOutcome::Complete
        } else {
            KubernetesDrainOutcome::Partial
        };

        KubernetesDrainReport::new(outcome, reports)
    }
}

impl std::fmt::Debug for KubernetesDrainController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubernetesDrainController")
            .field("health", &self.health)
            .field("step_count", &self.step_count())
            .finish()
    }
}

struct FnDrainStep<F> {
    name: String,
    run: F,
}

impl<F, Fut> KubernetesDrainStep for FnDrainStep<F>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = KubernetesDrainStepResult> + Send + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn run(&self) -> KubernetesDrainFuture {
        Box::pin((self.run)())
    }
}

fn stream_drain_result(
    result: Result<(), StreamError>,
    success_message: &'static str,
) -> KubernetesDrainStepResult {
    match result {
        Ok(()) => KubernetesDrainStepResult::completed(success_message),
        Err(StreamError::Closed) => KubernetesDrainStepResult::completed("stream already closed"),
        Err(StreamError::Cancelled { .. }) => {
            KubernetesDrainStepResult::completed("stream already cancelled")
        }
        Err(error) => KubernetesDrainStepResult::failed(error.to_string()),
    }
}

fn process_stop_completed(status: &ProcessActorStatus) -> KubernetesDrainStepResult {
    KubernetesDrainStepResult::completed(format!("process actor stopped: {:?}", status.state()))
}
