//! Autonomy policy and effect target catalog for durable agent runs.
//!
//! This module is intentionally product-neutral. It validates effect targets,
//! timer waits, skill-to-target policy, idempotency strategy, artifact policy,
//! and bounded autonomy budgets before work reaches dispatcher workers.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{
    AgentAttributes, AgentAuditEvent, AgentAuditEventId, AgentAuditEventKind, AgentCausationId,
    AgentCorrelationId, AgentDispatchTargetClass, AgentEffect, AgentEffectKind, AgentRunId,
    AgentTelemetryContext, AgentTenantId, AgentTimestampMillis, AgentWorkflowId, ArtifactKind,
    RedactionStatus, WorkflowDefinitionVersion,
};

/// Attribute recording the autonomy policy version that made a decision.
pub const AGENT_AUTONOMY_POLICY_VERSION_ATTRIBUTE: &str = "autonomy_policy_version";

/// Attribute recording the target class considered by autonomy policy.
pub const AGENT_AUTONOMY_TARGET_CLASS_ATTRIBUTE: &str = "autonomy_target_class";

/// Attribute recording the stable decision status.
pub const AGENT_AUTONOMY_DECISION_STATUS_ATTRIBUTE: &str = "autonomy_decision_status";

/// Attribute recording the stable reason code for a policy decision.
pub const AGENT_AUTONOMY_REASON_CODE_ATTRIBUTE: &str = "autonomy_reason_code";

/// Product-neutral effect target classes used by autonomy policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentAutonomyTargetClass {
    /// Model provider request.
    Model,
    /// In-process or remote tool adapter request.
    Tool,
    /// Supervised process-backed tool.
    ProcessTool,
    /// A2A peer-agent task call.
    A2aPeer,
    /// Human checkpoint notification.
    HumanCheckpoint,
    /// Durable timer wait.
    Timer,
    /// Generic webhook callback.
    Webhook,
    /// A2A push notification callback.
    PushNotification,
    /// Child workflow start command
    /// (agent-domain workflows-as-tools, slice 4.5).
    ChildWorkflow,
    /// Target that is not part of the supported catalog.
    Other,
}

impl AgentAutonomyTargetClass {
    /// Stable lowercase label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Tool => "tool",
            Self::ProcessTool => "process-tool",
            Self::A2aPeer => "a2a-peer",
            Self::HumanCheckpoint => "human-checkpoint",
            Self::Timer => "timer",
            Self::Webhook => "webhook",
            Self::PushNotification => "push-notification",
            Self::ChildWorkflow => "child-workflow",
            Self::Other => "other",
        }
    }

    /// Parses a stable target-class label.
    #[must_use]
    pub fn from_label(value: &str) -> Option<Self> {
        Some(match value {
            "model" => Self::Model,
            "tool" => Self::Tool,
            "process-tool" | "process" => Self::ProcessTool,
            "a2a-peer" => Self::A2aPeer,
            "human-checkpoint" | "human" => Self::HumanCheckpoint,
            "timer" => Self::Timer,
            "webhook" => Self::Webhook,
            "push-notification" | "push" | "a2a-push" => Self::PushNotification,
            "child-workflow" => Self::ChildWorkflow,
            "other" => Self::Other,
            _ => return None,
        })
    }

    /// Classifies one scheduled effect into a Phase 5 autonomy target class.
    ///
    /// The class is derived from the dispatcher's
    /// [`AgentDispatchTargetClass::classify`] so that policy admission and
    /// dispatch routing always agree on the class of one effect.
    #[must_use]
    pub fn for_effect(effect: &AgentEffect) -> Self {
        Self::from_dispatch_class(AgentDispatchTargetClass::classify(
            effect.kind,
            &effect.target,
        ))
    }

    /// Maps the dispatcher routing class onto the autonomy policy class.
    ///
    /// Dispatch classes outside the supported autonomy catalog map to
    /// [`Self::Other`], which fails closed against the default catalog.
    /// `ChildWorkflow` is first-class since agent-domain workflows-as-tools
    /// (slice 4.5): a workflow invoked as a tool carries explicit budgets,
    /// capabilities, and approval policy rather than the generic `Other`
    /// denial.
    #[must_use]
    pub const fn from_dispatch_class(class: AgentDispatchTargetClass) -> Self {
        match class {
            AgentDispatchTargetClass::Model => Self::Model,
            AgentDispatchTargetClass::Tool => Self::Tool,
            AgentDispatchTargetClass::Process => Self::ProcessTool,
            AgentDispatchTargetClass::A2aPeer => Self::A2aPeer,
            AgentDispatchTargetClass::Webhook => Self::Webhook,
            AgentDispatchTargetClass::PushNotification => Self::PushNotification,
            AgentDispatchTargetClass::Human => Self::HumanCheckpoint,
            AgentDispatchTargetClass::ChildWorkflow => Self::ChildWorkflow,
            AgentDispatchTargetClass::Http
            | AgentDispatchTargetClass::Grpc
            | AgentDispatchTargetClass::Notification
            | AgentDispatchTargetClass::Stream
            | AgentDispatchTargetClass::Artifact
            | AgentDispatchTargetClass::Audit
            | AgentDispatchTargetClass::Other => Self::Other,
        }
    }
}

/// Downstream idempotency strategy expected for a target class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentAutonomyIdempotencyPolicy {
    /// Use the stable durable effect id.
    EffectId,
    /// Use the durable outbox deduplication key.
    DeduplicationKey,
    /// Use the downstream idempotency key stored on the effect.
    EffectIdempotencyKey,
    /// Use the stable peer A2A message id or task idempotency key.
    A2aMessageId,
    /// Use the stable human checkpoint id.
    CheckpointId,
    /// Use the stable timer id.
    TimerId,
    /// Use a target-scoped event key such as task-id/event-sequence/config-id.
    TargetEventKey,
}

impl AgentAutonomyIdempotencyPolicy {
    /// Stable lowercase label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::EffectId => "effect-id",
            Self::DeduplicationKey => "deduplication-key",
            Self::EffectIdempotencyKey => "effect-idempotency-key",
            Self::A2aMessageId => "a2a-message-id",
            Self::CheckpointId => "checkpoint-id",
            Self::TimerId => "timer-id",
            Self::TargetEventKey => "target-event-key",
        }
    }
}

/// Artifact handling required for effect input or output payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentAutonomyArtifactPolicy {
    /// No payload is expected.
    NoPayload,
    /// Small inline payloads are permitted by a higher-level bounded policy.
    InlineAllowed,
    /// Artifact references are preferred for large payloads.
    ArtifactPreferred,
    /// An artifact reference is required before scheduling.
    ArtifactRequired,
}

impl AgentAutonomyArtifactPolicy {
    /// Stable lowercase label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::NoPayload => "no-payload",
            Self::InlineAllowed => "inline-allowed",
            Self::ArtifactPreferred => "artifact-preferred",
            Self::ArtifactRequired => "artifact-required",
        }
    }
}

/// Input and output artifact policy for one target class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAutonomyArtifactPolicies {
    /// Input payload policy.
    pub input: AgentAutonomyArtifactPolicy,
    /// Output payload policy.
    pub output: AgentAutonomyArtifactPolicy,
}

impl AgentAutonomyArtifactPolicies {
    /// Creates input and output artifact policies.
    #[must_use]
    pub const fn new(
        input: AgentAutonomyArtifactPolicy,
        output: AgentAutonomyArtifactPolicy,
    ) -> Self {
        Self { input, output }
    }
}

/// Descriptor for one supported target class.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAutonomyTargetClassDescriptor {
    /// Target class being described.
    pub target_class: AgentAutonomyTargetClass,
    /// Effect kinds accepted for this target class.
    pub allowed_effect_kinds: Vec<AgentEffectKind>,
    /// Downstream idempotency strategy.
    pub idempotency: AgentAutonomyIdempotencyPolicy,
    /// Input and output artifact policies.
    pub artifacts: AgentAutonomyArtifactPolicies,
}

impl AgentAutonomyTargetClassDescriptor {
    /// Creates a target class descriptor.
    #[must_use]
    pub fn new(
        target_class: AgentAutonomyTargetClass,
        allowed_effect_kinds: impl IntoIterator<Item = AgentEffectKind>,
        idempotency: AgentAutonomyIdempotencyPolicy,
        artifacts: AgentAutonomyArtifactPolicies,
    ) -> Self {
        Self {
            target_class,
            allowed_effect_kinds: allowed_effect_kinds.into_iter().collect(),
            idempotency,
            artifacts,
        }
    }

    /// Returns true when this descriptor accepts the effect kind.
    #[must_use]
    pub fn allows_effect_kind(&self, kind: AgentEffectKind) -> bool {
        self.allowed_effect_kinds.contains(&kind)
    }
}

/// Registry of supported target classes plus optional public skill policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEffectTargetCatalog {
    descriptors: BTreeMap<AgentAutonomyTargetClass, AgentAutonomyTargetClassDescriptor>,
    skill_targets: BTreeMap<String, BTreeSet<AgentAutonomyTargetClass>>,
}

impl AgentEffectTargetCatalog {
    /// Creates an empty catalog. An empty catalog fails closed.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            descriptors: BTreeMap::new(),
            skill_targets: BTreeMap::new(),
        }
    }

    /// Creates the Phase 5 target catalog.
    #[must_use]
    pub fn phase5_default() -> Self {
        use AgentAutonomyArtifactPolicy::{
            ArtifactPreferred, ArtifactRequired, InlineAllowed, NoPayload,
        };
        use AgentAutonomyIdempotencyPolicy::{
            A2aMessageId, CheckpointId, DeduplicationKey, EffectIdempotencyKey, TargetEventKey,
            TimerId,
        };

        Self::empty()
            .with_descriptor(AgentAutonomyTargetClassDescriptor::new(
                AgentAutonomyTargetClass::Model,
                [AgentEffectKind::ModelCall],
                EffectIdempotencyKey,
                AgentAutonomyArtifactPolicies::new(ArtifactRequired, ArtifactRequired),
            ))
            .with_descriptor(AgentAutonomyTargetClassDescriptor::new(
                AgentAutonomyTargetClass::Tool,
                [AgentEffectKind::ToolCall],
                EffectIdempotencyKey,
                AgentAutonomyArtifactPolicies::new(ArtifactPreferred, ArtifactRequired),
            ))
            .with_descriptor(AgentAutonomyTargetClassDescriptor::new(
                AgentAutonomyTargetClass::ProcessTool,
                [AgentEffectKind::ProcessCall],
                EffectIdempotencyKey,
                AgentAutonomyArtifactPolicies::new(ArtifactPreferred, ArtifactRequired),
            ))
            // `ToolCall` and the relaxed input policy admit the agent
            // domain's outbound A2A sends (slice 4.3): they ride the
            // executor-routed tool family with target type `a2a-peer` and
            // carry inline delegation payloads rather than an input
            // artifact, and the dispatcher classifies them as `A2aPeer` —
            // a catalog that refused the kind would deny exactly the sends
            // the classification routes.
            .with_descriptor(AgentAutonomyTargetClassDescriptor::new(
                AgentAutonomyTargetClass::A2aPeer,
                [
                    AgentEffectKind::HttpCall,
                    AgentEffectKind::GrpcCall,
                    AgentEffectKind::ToolCall,
                ],
                A2aMessageId,
                AgentAutonomyArtifactPolicies::new(ArtifactPreferred, ArtifactRequired),
            ))
            .with_descriptor(AgentAutonomyTargetClassDescriptor::new(
                AgentAutonomyTargetClass::HumanCheckpoint,
                [AgentEffectKind::HumanApprovalRequest],
                CheckpointId,
                AgentAutonomyArtifactPolicies::new(ArtifactPreferred, InlineAllowed),
            ))
            .with_descriptor(AgentAutonomyTargetClassDescriptor::new(
                AgentAutonomyTargetClass::Webhook,
                [AgentEffectKind::HttpCall, AgentEffectKind::Notification],
                EffectIdempotencyKey,
                AgentAutonomyArtifactPolicies::new(ArtifactPreferred, ArtifactPreferred),
            ))
            .with_descriptor(AgentAutonomyTargetClassDescriptor::new(
                AgentAutonomyTargetClass::PushNotification,
                [AgentEffectKind::Notification],
                TargetEventKey,
                AgentAutonomyArtifactPolicies::new(InlineAllowed, NoPayload),
            ))
            .with_descriptor(AgentAutonomyTargetClassDescriptor::new(
                AgentAutonomyTargetClass::Timer,
                [],
                TimerId,
                AgentAutonomyArtifactPolicies::new(NoPayload, InlineAllowed),
            ))
            // A child-workflow start converges on the outbox deduplication
            // key: it is the derived invocation identity the child run's own
            // inbox deduplicates on, so every replay adopts the one child.
            // `ToolCall` is accepted because agent-domain workflows-as-tools
            // (slice 4.5) ride the executor-routed tool family with target
            // type `workflow-tool`, which the dispatcher classifies as
            // `ChildWorkflow`.
            .with_descriptor(AgentAutonomyTargetClassDescriptor::new(
                AgentAutonomyTargetClass::ChildWorkflow,
                [
                    AgentEffectKind::ChildWorkflowCommand,
                    AgentEffectKind::ToolCall,
                ],
                DeduplicationKey,
                AgentAutonomyArtifactPolicies::new(ArtifactPreferred, ArtifactPreferred),
            ))
    }

    /// Inserts or replaces one target-class descriptor.
    #[must_use]
    pub fn with_descriptor(mut self, descriptor: AgentAutonomyTargetClassDescriptor) -> Self {
        self.descriptors.insert(descriptor.target_class, descriptor);
        self
    }

    /// Maps a public skill to allowed target classes.
    #[must_use]
    pub fn with_skill_targets(
        mut self,
        skill: impl Into<String>,
        classes: impl IntoIterator<Item = AgentAutonomyTargetClass>,
    ) -> Self {
        self.skill_targets
            .insert(skill.into(), classes.into_iter().collect());
        self
    }

    /// Returns one target-class descriptor.
    #[must_use]
    pub fn descriptor(
        &self,
        target_class: AgentAutonomyTargetClass,
    ) -> Option<&AgentAutonomyTargetClassDescriptor> {
        self.descriptors.get(&target_class)
    }

    /// Returns true when the skill policy allows the target class.
    #[must_use]
    pub fn skill_allows(
        &self,
        skill: Option<&str>,
        target_class: AgentAutonomyTargetClass,
    ) -> bool {
        skill.is_none_or(|skill| {
            self.skill_targets
                .get(skill)
                .is_some_and(|allowed| allowed.contains(&target_class))
        })
    }

    /// Validates one effect target against the catalog and autonomy policy.
    pub fn validate_effect(
        &self,
        policy: &AgentAutonomyPolicy,
        usage: &AgentAutonomyUsage,
        effect: &AgentEffect,
        skill: Option<&str>,
        now: AgentTimestampMillis,
    ) -> AgentAutonomyPolicyResult<AgentAutonomyPolicyDecision> {
        let target_class = AgentAutonomyTargetClass::for_effect(effect);
        let Some(descriptor) = self.descriptor(target_class) else {
            return Ok(policy_decision(
                policy,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::Denied,
                "unsupported-target-class",
                "target class is not present in the effect catalog",
                now,
            ));
        };
        if !descriptor.allows_effect_kind(effect.kind) {
            return Ok(policy_decision(
                policy,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::Denied,
                "effect-kind-not-supported",
                "effect kind is not supported for target class",
                now,
            ));
        }
        if !policy.allowed_target_classes.contains(&target_class) {
            return Ok(policy_decision(
                policy,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::Denied,
                "target-class-disallowed",
                "target class is not allowed by workflow policy",
                now,
            ));
        }
        if !self.skill_allows(skill, target_class) {
            return Ok(policy_decision(
                policy,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::Denied,
                "skill-target-class-disallowed",
                "public skill is not allowed to schedule this target class",
                now,
            ));
        }
        if tool_name_is_disallowed(policy, target_class, &effect.target.name) {
            return Ok(policy_decision(
                policy,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::Denied,
                "tool-disallowed",
                "tool target is not allowed by workflow policy",
                now,
            ));
        }
        if descriptor.artifacts.input == AgentAutonomyArtifactPolicy::ArtifactRequired
            && effect.payload_ref.is_none()
        {
            return Ok(policy_decision(
                policy,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::Denied,
                "input-artifact-required",
                "target class requires an input artifact reference",
                now,
            ));
        }
        Ok(policy.evaluate_target(target_class, usage, now))
    }

    /// Validates a durable timer wait against the catalog and policy.
    pub fn validate_timer(
        &self,
        policy: &AgentAutonomyPolicy,
        usage: &AgentAutonomyUsage,
        now: AgentTimestampMillis,
    ) -> AgentAutonomyPolicyResult<AgentAutonomyPolicyDecision> {
        if self.descriptor(AgentAutonomyTargetClass::Timer).is_none() {
            return Ok(policy_decision(
                policy,
                Some(AgentAutonomyTargetClass::Timer),
                AgentAutonomyPolicyDecisionStatus::Denied,
                "unsupported-target-class",
                "timer target class is not present in the effect catalog",
                now,
            ));
        }
        Ok(policy.evaluate_target(AgentAutonomyTargetClass::Timer, usage, now))
    }
}

impl Default for AgentEffectTargetCatalog {
    fn default() -> Self {
        Self::phase5_default()
    }
}

/// Durable usage counters considered by autonomy policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAutonomyUsage {
    /// Autonomous scheduler steps already taken by the run.
    pub autonomous_steps: u64,
    /// External calls already attempted by the run.
    pub external_calls: u64,
    /// Tokens already consumed by model calls.
    pub tokens: u64,
    /// Wall-clock start timestamp for the autonomous segment.
    pub started_at: Option<AgentTimestampMillis>,
}

impl AgentAutonomyUsage {
    /// Creates zeroed usage counters.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            autonomous_steps: 0,
            external_calls: 0,
            tokens: 0,
            started_at: None,
        }
    }

    /// Sets autonomous step count.
    #[must_use]
    pub const fn autonomous_steps(mut self, autonomous_steps: u64) -> Self {
        self.autonomous_steps = autonomous_steps;
        self
    }

    /// Sets external call count.
    #[must_use]
    pub const fn external_calls(mut self, external_calls: u64) -> Self {
        self.external_calls = external_calls;
        self
    }

    /// Sets token count.
    #[must_use]
    pub const fn tokens(mut self, tokens: u64) -> Self {
        self.tokens = tokens;
        self
    }

    /// Sets autonomous segment start timestamp.
    #[must_use]
    pub const fn started_at(mut self, started_at: AgentTimestampMillis) -> Self {
        self.started_at = Some(started_at);
        self
    }
}

/// Workflow-level autonomy policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAutonomyPolicy {
    /// Stable policy version persisted with decisions and audit records.
    pub policy_version: String,
    /// Target classes allowed by this workflow.
    pub allowed_target_classes: BTreeSet<AgentAutonomyTargetClass>,
    /// Optional allowlist for tool and process-tool target names.
    pub allowed_tool_names: BTreeSet<String>,
    /// Target classes requiring approval before scheduling.
    pub approval_required_target_classes: BTreeSet<AgentAutonomyTargetClass>,
    /// Maximum autonomous scheduler steps.
    pub max_autonomous_steps: Option<u64>,
    /// Maximum wall-clock duration in milliseconds.
    pub max_wall_clock_ms: Option<u64>,
    /// Maximum external calls.
    pub max_external_calls: Option<u64>,
    /// Maximum model tokens.
    pub max_tokens: Option<u64>,
    /// Durable cancellation requested flag.
    pub cancellation_requested: bool,
}

impl AgentAutonomyPolicy {
    /// Creates a fail-closed policy with no allowed target classes.
    #[must_use]
    pub fn fail_closed(policy_version: impl Into<String>) -> Self {
        Self {
            policy_version: policy_version.into(),
            allowed_target_classes: BTreeSet::new(),
            allowed_tool_names: BTreeSet::new(),
            approval_required_target_classes: BTreeSet::new(),
            max_autonomous_steps: None,
            max_wall_clock_ms: None,
            max_external_calls: None,
            max_tokens: None,
            cancellation_requested: false,
        }
    }

    /// Creates a Phase 5 default policy that allows every supported class.
    #[must_use]
    pub fn phase5_default(policy_version: impl Into<String>) -> Self {
        Self::fail_closed(policy_version)
            .allow_target_class(AgentAutonomyTargetClass::Model)
            .allow_target_class(AgentAutonomyTargetClass::Tool)
            .allow_target_class(AgentAutonomyTargetClass::ProcessTool)
            .allow_target_class(AgentAutonomyTargetClass::A2aPeer)
            .allow_target_class(AgentAutonomyTargetClass::HumanCheckpoint)
            .allow_target_class(AgentAutonomyTargetClass::Timer)
            .allow_target_class(AgentAutonomyTargetClass::Webhook)
            .allow_target_class(AgentAutonomyTargetClass::PushNotification)
            .allow_target_class(AgentAutonomyTargetClass::ChildWorkflow)
    }

    /// Allows one target class.
    #[must_use]
    pub fn allow_target_class(mut self, target_class: AgentAutonomyTargetClass) -> Self {
        self.allowed_target_classes.insert(target_class);
        self
    }

    /// Allows one tool or process-tool target name.
    #[must_use]
    pub fn allow_tool(mut self, tool_name: impl Into<String>) -> Self {
        self.allowed_tool_names.insert(tool_name.into());
        self
    }

    /// Requires approval for one target class.
    #[must_use]
    pub fn require_approval_for(mut self, target_class: AgentAutonomyTargetClass) -> Self {
        self.approval_required_target_classes.insert(target_class);
        self
    }

    /// Sets maximum autonomous steps.
    #[must_use]
    pub const fn max_autonomous_steps(mut self, max_autonomous_steps: u64) -> Self {
        self.max_autonomous_steps = Some(max_autonomous_steps);
        self
    }

    /// Sets maximum wall-clock duration.
    #[must_use]
    pub const fn max_wall_clock_ms(mut self, max_wall_clock_ms: u64) -> Self {
        self.max_wall_clock_ms = Some(max_wall_clock_ms);
        self
    }

    /// Sets maximum external call count.
    #[must_use]
    pub const fn max_external_calls(mut self, max_external_calls: u64) -> Self {
        self.max_external_calls = Some(max_external_calls);
        self
    }

    /// Sets maximum model token count.
    #[must_use]
    pub const fn max_tokens(mut self, max_tokens: u64) -> Self {
        self.max_tokens = Some(max_tokens);
        self
    }

    /// Marks durable cancellation requested.
    #[must_use]
    pub const fn cancellation_requested(mut self) -> Self {
        self.cancellation_requested = true;
        self
    }

    /// Evaluates budget, approval, and cancellation policy for a target class.
    #[must_use]
    pub fn evaluate_target(
        &self,
        target_class: AgentAutonomyTargetClass,
        usage: &AgentAutonomyUsage,
        now: AgentTimestampMillis,
    ) -> AgentAutonomyPolicyDecision {
        if self.cancellation_requested {
            return policy_decision(
                self,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::Cancelled,
                "cancellation-requested",
                "run cancellation has been requested",
                now,
            );
        }
        if usage_exceeds(usage.autonomous_steps, self.max_autonomous_steps) {
            return policy_decision(
                self,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::Denied,
                "max-autonomous-steps-exceeded",
                "autonomous step budget is exhausted",
                now,
            );
        }
        if usage_exceeds(usage.external_calls, self.max_external_calls) {
            return policy_decision(
                self,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::Denied,
                "max-external-calls-exceeded",
                "external call budget is exhausted",
                now,
            );
        }
        if usage_exceeds(usage.tokens, self.max_tokens) {
            return policy_decision(
                self,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::Denied,
                "max-tokens-exceeded",
                "token budget is exhausted",
                now,
            );
        }
        if wall_clock_exceeded(usage.started_at, self.max_wall_clock_ms, now) {
            return policy_decision(
                self,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::Denied,
                "wall-clock-timeout-exceeded",
                "wall-clock autonomy budget is exhausted",
                now,
            );
        }
        if self
            .approval_required_target_classes
            .contains(&target_class)
        {
            return policy_decision(
                self,
                Some(target_class),
                AgentAutonomyPolicyDecisionStatus::ApprovalRequired,
                "approval-required",
                "target class requires approval",
                now,
            );
        }
        policy_decision(
            self,
            Some(target_class),
            AgentAutonomyPolicyDecisionStatus::Allowed,
            "allowed",
            "target is allowed by autonomy policy",
            now,
        )
    }
}

/// Policy decision status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentAutonomyPolicyDecisionStatus {
    /// Target may be scheduled immediately.
    Allowed,
    /// Target was rejected before scheduling.
    Denied,
    /// Target requires approval before scheduling.
    ApprovalRequired,
    /// Target was rejected because cancellation was requested.
    Cancelled,
}

impl AgentAutonomyPolicyDecisionStatus {
    /// Stable lowercase label.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Denied => "denied",
            Self::ApprovalRequired => "approval-required",
            Self::Cancelled => "cancelled",
        }
    }
}

/// One persisted or auditable policy decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAutonomyPolicyDecision {
    /// Stable policy version.
    pub policy_version: String,
    /// Decision status.
    pub status: AgentAutonomyPolicyDecisionStatus,
    /// Target class considered by the decision.
    pub target_class: Option<AgentAutonomyTargetClass>,
    /// Stable bounded reason code.
    pub reason_code: String,
    /// Human-readable bounded reason summary.
    pub reason_summary: String,
    /// Decision timestamp.
    pub decided_at: AgentTimestampMillis,
    /// Bounded decision attributes.
    pub attributes: AgentAttributes,
}

impl AgentAutonomyPolicyDecision {
    /// Returns true when the decision allows scheduling.
    #[must_use]
    pub const fn is_allowed(&self) -> bool {
        matches!(self.status, AgentAutonomyPolicyDecisionStatus::Allowed)
    }

    /// Returns true when the decision rejects scheduling.
    #[must_use]
    pub const fn is_rejected(&self) -> bool {
        matches!(
            self.status,
            AgentAutonomyPolicyDecisionStatus::Denied
                | AgentAutonomyPolicyDecisionStatus::Cancelled
        )
    }

    /// Converts the decision to bounded attributes for run metadata, audit, or projections.
    #[must_use]
    pub fn as_attributes(&self) -> AgentAttributes {
        let mut attributes = self.attributes.clone();
        attributes.insert(
            AGENT_AUTONOMY_POLICY_VERSION_ATTRIBUTE.to_string(),
            self.policy_version.clone(),
        );
        attributes.insert(
            AGENT_AUTONOMY_DECISION_STATUS_ATTRIBUTE.to_string(),
            self.status.as_label().to_string(),
        );
        attributes.insert(
            AGENT_AUTONOMY_REASON_CODE_ATTRIBUTE.to_string(),
            self.reason_code.clone(),
        );
        if let Some(target_class) = self.target_class {
            attributes.insert(
                AGENT_AUTONOMY_TARGET_CLASS_ATTRIBUTE.to_string(),
                target_class.as_label().to_string(),
            );
        }
        attributes
    }
}

/// Shared result type for autonomy policy operations.
pub type AgentAutonomyPolicyResult<T> = Result<T, AgentAutonomyPolicyError>;

/// Stable autonomy policy failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentAutonomyPolicyError {
    /// A required policy or catalog value is invalid.
    InvalidPolicy {
        /// Invalid field.
        field: &'static str,
        /// Stable reason.
        reason: &'static str,
    },
}

impl AgentAutonomyPolicyError {
    /// Stable machine-readable error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidPolicy { .. } => "invalid-autonomy-policy",
        }
    }
}

impl Display for AgentAutonomyPolicyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy { field, reason } => {
                write!(f, "invalid autonomy policy field {field}: {reason}")
            }
        }
    }
}

impl Error for AgentAutonomyPolicyError {}

/// Builds an audit event from an autonomy policy decision.
#[allow(clippy::too_many_arguments)]
pub fn agent_autonomy_policy_audit_event(
    audit_event_id: AgentAuditEventId,
    workflow_id: AgentWorkflowId,
    run_id: AgentRunId,
    definition_version: WorkflowDefinitionVersion,
    tenant: Option<AgentTenantId>,
    decision: AgentAutonomyPolicyDecision,
    causation_id: AgentCausationId,
    correlation_id: AgentCorrelationId,
    telemetry_context: AgentTelemetryContext,
) -> AgentAuditEvent {
    AgentAuditEvent {
        audit_event_id,
        kind: AgentAuditEventKind::PolicyOverride,
        workflow_id,
        run_id,
        definition_version,
        tenant,
        step_id: None,
        effect_id: None,
        checkpoint_id: None,
        command_id: None,
        causation_id,
        correlation_id,
        actor_principal: None,
        artifact_refs: Vec::new(),
        content_hashes: AgentAttributes::new(),
        redaction: RedactionStatus::ReferenceOnly,
        telemetry_context,
        occurred_at: decision.decided_at,
        attributes: decision.as_attributes(),
    }
}

fn policy_decision(
    policy: &AgentAutonomyPolicy,
    target_class: Option<AgentAutonomyTargetClass>,
    status: AgentAutonomyPolicyDecisionStatus,
    reason_code: &'static str,
    reason_summary: &'static str,
    now: AgentTimestampMillis,
) -> AgentAutonomyPolicyDecision {
    AgentAutonomyPolicyDecision {
        policy_version: policy.policy_version.clone(),
        status,
        target_class,
        reason_code: reason_code.to_string(),
        reason_summary: reason_summary.to_string(),
        decided_at: now,
        attributes: AgentAttributes::new(),
    }
}

fn tool_name_is_disallowed(
    policy: &AgentAutonomyPolicy,
    target_class: AgentAutonomyTargetClass,
    tool_name: &str,
) -> bool {
    matches!(
        target_class,
        AgentAutonomyTargetClass::Tool | AgentAutonomyTargetClass::ProcessTool
    ) && !policy.allowed_tool_names.is_empty()
        && !policy.allowed_tool_names.contains(tool_name)
}

fn usage_exceeds(observed: u64, limit: Option<u64>) -> bool {
    limit.is_some_and(|limit| observed >= limit)
}

fn wall_clock_exceeded(
    started_at: Option<AgentTimestampMillis>,
    limit_ms: Option<u64>,
    now: AgentTimestampMillis,
) -> bool {
    match (started_at, limit_ms) {
        (Some(started_at), Some(limit_ms)) => {
            now.as_millis().saturating_sub(started_at.as_millis()) >= limit_ms
        }
        _ => false,
    }
}

/// Returns true when an artifact kind is usually an input to an autonomous target.
#[must_use]
pub const fn is_autonomy_input_artifact(kind: ArtifactKind) -> bool {
    matches!(
        kind,
        ArtifactKind::Input | ArtifactKind::Prompt | ArtifactKind::File
    )
}
