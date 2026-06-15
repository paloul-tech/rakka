//! Kubernetes pre-stop drain orchestration.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rakka_cluster::MembershipState;
use rakka_core::{
    ActorRef, CoordinatedShutdown, CoordinatedShutdownError, CoordinatedShutdownReason,
    CoordinatedShutdownReport, RakkaError, RakkaResult, ShutdownOutcome, ShutdownPhase,
    ShutdownTaskOptions, ShutdownTaskStatus, Subsystem,
};
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

    /// Maps a coordinated shutdown report into the Kubernetes pre-stop report shape.
    #[must_use]
    pub fn from_coordinated_shutdown_report(report: &CoordinatedShutdownReport) -> Self {
        Self::from_coordinated_shutdown_report_with_names(report, &[])
    }

    fn from_coordinated_shutdown_report_with_names(
        report: &CoordinatedShutdownReport,
        step_names: &[CoordinatedDrainStepTask],
    ) -> Self {
        let mut steps = Vec::new();
        for phase in report.phases() {
            if phase.tasks().is_empty() && phase.outcome() == ShutdownOutcome::TimedOut {
                steps.push(KubernetesDrainStepReport::new(
                    phase.phase().name(),
                    KubernetesDrainStepStatus::TimedOut,
                    format!(
                        "coordinated-shutdown reason={} phase={} status=timed-out",
                        report.reason().code(),
                        phase.phase().name()
                    ),
                ));
            }

            for task in phase.tasks() {
                let name = coordinated_step_name(step_names, task.phase(), task.task_name());
                steps.push(KubernetesDrainStepReport::new(
                    name,
                    kubernetes_status_from_shutdown_status(task.status()),
                    coordinated_task_message(report, task),
                ));
            }
        }

        if steps.is_empty() {
            steps.push(KubernetesDrainStepReport::new(
                "coordinated-shutdown",
                if report.outcome() == ShutdownOutcome::TimedOut {
                    KubernetesDrainStepStatus::TimedOut
                } else if report.outcome() == ShutdownOutcome::Complete {
                    KubernetesDrainStepStatus::Completed
                } else {
                    KubernetesDrainStepStatus::Failed
                },
                format!(
                    "coordinated-shutdown reason={} outcome={}",
                    report.reason().code(),
                    report.outcome().as_str()
                ),
            ));
        }

        Self::new(
            kubernetes_outcome_from_shutdown_outcome(report.outcome()),
            steps,
        )
    }
}

/// Pre-stop drain controller that marks readiness false and runs registered hooks.
#[derive(Clone)]
pub struct KubernetesDrainController {
    health: KubernetesNodeHealth,
    steps: Vec<Arc<dyn KubernetesDrainStep>>,
    coordinated_shutdown: Option<CoordinatedShutdown>,
    coordinated_step_tasks: Vec<CoordinatedDrainStepTask>,
    coordinated_registration_errors: Vec<KubernetesDrainStepReport>,
}

impl KubernetesDrainController {
    /// Creates a drain controller for the provided health model.
    #[must_use]
    pub fn new(health: KubernetesNodeHealth) -> Self {
        Self {
            health,
            steps: Vec::new(),
            coordinated_shutdown: None,
            coordinated_step_tasks: Vec::new(),
            coordinated_registration_errors: Vec::new(),
        }
    }

    /// Creates a drain controller backed by a coordinated shutdown registry.
    ///
    /// Existing custom drain steps added through [`Self::add_step`] are wrapped
    /// as coordinated shutdown tasks in the `drain-adapters` phase. Calling
    /// [`Self::drain`] marks readiness false and then runs the shared shutdown
    /// path with reason `kubernetes-prestop`.
    #[must_use]
    pub fn from_coordinated_shutdown(
        health: KubernetesNodeHealth,
        shutdown: CoordinatedShutdown,
    ) -> Self {
        Self {
            health,
            steps: Vec::new(),
            coordinated_shutdown: Some(shutdown),
            coordinated_step_tasks: Vec::new(),
            coordinated_registration_errors: Vec::new(),
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
        let step: Arc<dyn KubernetesDrainStep> = Arc::new(FnDrainStep {
            name: name.into(),
            run,
        });
        self.register_coordinated_step(step.clone());
        self.steps.push(step);
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
        if let Some(shutdown) = &self.coordinated_shutdown {
            return self.drain_coordinated(shutdown.clone(), timeout).await;
        }

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

    async fn drain_coordinated(
        &self,
        shutdown: CoordinatedShutdown,
        timeout: Duration,
    ) -> KubernetesDrainReport {
        let deadline = Instant::now() + timeout;
        let result = shutdown
            .run_with_deadline(CoordinatedShutdownReason::kubernetes_prestop(), deadline)
            .await;
        let mut report = match result {
            Ok(report) => KubernetesDrainReport::from_coordinated_shutdown_report_with_names(
                &report,
                &self.coordinated_step_tasks,
            ),
            Err(CoordinatedShutdownError::Failed { report })
            | Err(CoordinatedShutdownError::TimedOut { report }) => {
                KubernetesDrainReport::from_coordinated_shutdown_report_with_names(
                    &report,
                    &self.coordinated_step_tasks,
                )
            }
            Err(CoordinatedShutdownError::Registry { error }) => KubernetesDrainReport::new(
                KubernetesDrainOutcome::Partial,
                vec![KubernetesDrainStepReport::new(
                    "coordinated-shutdown-registry",
                    KubernetesDrainStepStatus::Failed,
                    error.to_string(),
                )],
            ),
        };

        if !self.coordinated_registration_errors.is_empty() {
            let mut steps = report.steps().to_vec();
            steps.extend(self.coordinated_registration_errors.iter().cloned());
            let outcome = if report.outcome() == KubernetesDrainOutcome::TimedOut {
                KubernetesDrainOutcome::TimedOut
            } else {
                KubernetesDrainOutcome::Partial
            };
            report = KubernetesDrainReport::new(outcome, steps);
        }

        report
    }

    fn register_coordinated_step(&mut self, step: Arc<dyn KubernetesDrainStep>) {
        let Some(shutdown) = &self.coordinated_shutdown else {
            return;
        };
        let task = CoordinatedDrainStepTask::new(
            step.name(),
            self.coordinated_step_tasks.len(),
            ShutdownPhase::drain_adapters(),
        );
        let options = match kubernetes_drain_step_options(step.name()) {
            Ok(options) => options,
            Err(error) => {
                self.coordinated_registration_errors
                    .push(KubernetesDrainStepReport::new(
                        step.name(),
                        KubernetesDrainStepStatus::Failed,
                        error.to_string(),
                    ));
                return;
            }
        };
        let task_name = task.task_name.clone();
        let step_for_task = step.clone();
        match shutdown.add_task_with_options(
            task.phase.clone(),
            task_name,
            options,
            move |_context| {
                let step = step_for_task.clone();
                async move { run_legacy_drain_step_as_shutdown_task(step).await }
            },
        ) {
            Ok(_task) => self.coordinated_step_tasks.push(task),
            Err(error) => {
                self.coordinated_registration_errors
                    .push(KubernetesDrainStepReport::new(
                        step.name(),
                        KubernetesDrainStepStatus::Failed,
                        error.to_string(),
                    ))
            }
        }
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct CoordinatedDrainStepTask {
    step_name: String,
    task_name: String,
    phase: ShutdownPhase,
}

impl CoordinatedDrainStepTask {
    fn new(step_name: &str, index: usize, phase: ShutdownPhase) -> Self {
        Self {
            step_name: step_name.to_owned(),
            task_name: format!(
                "k8s-drain-step-{index}-{}",
                shutdown_name_fragment(step_name)
            ),
            phase,
        }
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

async fn run_legacy_drain_step_as_shutdown_task(
    step: Arc<dyn KubernetesDrainStep>,
) -> RakkaResult<()> {
    match step.run().await {
        KubernetesDrainStepResult::Completed { .. } => Ok(()),
        KubernetesDrainStepResult::Failed { message } => Err(RakkaError::new(
            Subsystem::K8s,
            "kubernetes-drain-step-failed",
            message,
        )),
    }
}

fn kubernetes_drain_step_options(step_name: &str) -> RakkaResult<ShutdownTaskOptions> {
    ShutdownTaskOptions::default()
        .with_attribute("operation", "kubernetes-drain-step")?
        .with_attribute("kubernetes-step", step_name)
}

fn shutdown_name_fragment(name: &str) -> String {
    let fragment = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_owned();
    if fragment.is_empty() {
        "step".to_owned()
    } else {
        fragment
    }
}

fn coordinated_step_name(
    step_names: &[CoordinatedDrainStepTask],
    phase: &ShutdownPhase,
    task_name: &str,
) -> String {
    step_names
        .iter()
        .find(|task| task.phase == *phase && task.task_name == task_name)
        .map_or_else(
            || format!("{}/{}", phase.name(), task_name),
            |task| task.step_name.clone(),
        )
}

fn coordinated_task_message(
    report: &CoordinatedShutdownReport,
    task: &rakka_core::ShutdownTaskReport,
) -> String {
    let status = task.status().as_str();
    let mut message = format!(
        "coordinated-shutdown reason={} phase={} task={} status={status}",
        report.reason().code(),
        task.phase().name(),
        task.task_name(),
    );
    if let Some(detail) = task.message() {
        message.push_str(": ");
        message.push_str(detail);
    }
    message
}

fn kubernetes_status_from_shutdown_status(status: ShutdownTaskStatus) -> KubernetesDrainStepStatus {
    match status {
        ShutdownTaskStatus::Completed => KubernetesDrainStepStatus::Completed,
        ShutdownTaskStatus::Failed => KubernetesDrainStepStatus::Failed,
        ShutdownTaskStatus::TimedOut
        | ShutdownTaskStatus::Pending
        | ShutdownTaskStatus::Running
        | ShutdownTaskStatus::Skipped => KubernetesDrainStepStatus::TimedOut,
    }
}

fn kubernetes_outcome_from_shutdown_outcome(outcome: ShutdownOutcome) -> KubernetesDrainOutcome {
    match outcome {
        ShutdownOutcome::Complete => KubernetesDrainOutcome::Complete,
        ShutdownOutcome::TimedOut => KubernetesDrainOutcome::TimedOut,
        ShutdownOutcome::NotStarted
        | ShutdownOutcome::Running
        | ShutdownOutcome::Partial
        | ShutdownOutcome::Failed => KubernetesDrainOutcome::Partial,
    }
}
