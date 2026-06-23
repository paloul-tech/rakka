//! Product-neutral compiled execution plan contracts.
//!
//! Application backends compile editor-owned workflow DSLs into these durable
//! runtime contracts. Rakka interprets the compiled plan; it does not own the
//! editor DSL, UI layout, credential storage, trigger registration, or product
//! policy.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::metrics::label_value_contains_line_break;
use crate::{
    AgentAttributes, AgentWorkflowId, ArtifactRef, WorkflowDefinitionVersion,
    AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES, FORBIDDEN_HOT_METRIC_FIELDS,
};

const DEFAULT_COMPILED_PLAN_RUNTIME_VERSION: &str = "0.1.0";
const COMPILED_GRAPH_V1_CAPABILITY: &str = "compiled-graph-v1";

/// Current compiled execution plan schema version supported by registration.
pub const CURRENT_AGENT_COMPILED_PLAN_SCHEMA_VERSION: AgentCompiledPlanSchemaVersion =
    AgentCompiledPlanSchemaVersion::new(1);

macro_rules! string_id {
    ($(#[$meta:meta])* $vis:vis $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        $vis struct $name(String);

        impl $name {
            /// Creates a new identifier.
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns this identifier as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes this identifier and returns its owned string.
            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

string_id! {
    /// Stable identity for one compiled execution plan.
    pub AgentCompiledPlanId
}

string_id! {
    /// Stable fingerprint for one immutable compiled execution plan.
    pub AgentCompiledPlanFingerprint
}

string_id! {
    /// Stable identity for one node in a compiled execution plan.
    pub AgentCompiledNodeId
}

string_id! {
    /// Stable identity for one edge in a compiled execution plan.
    pub AgentCompiledEdgeId
}

string_id! {
    /// Stable identity for one node input or output port.
    pub AgentCompiledPortId
}

string_id! {
    /// Logical application-owned reference to a third-party credential binding.
    ///
    /// This reference must not contain an API key, OAuth token, password, or
    /// other secret value. Application code resolves it at dispatch time.
    pub AgentCredentialBindingRef
}

/// Serialized compiled execution plan schema version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentCompiledPlanSchemaVersion(u32);

impl AgentCompiledPlanSchemaVersion {
    /// Creates a compiled plan schema version from a positive integer.
    #[must_use]
    pub const fn new(version: u32) -> Self {
        Self(version)
    }

    /// Returns the version number.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Product-neutral compiled execution plan interpreted by Rakka.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledExecutionPlan {
    /// Stable plan id.
    pub plan_id: AgentCompiledPlanId,
    /// Workflow definition id this plan implements.
    pub workflow_id: AgentWorkflowId,
    /// Bounded workflow type label.
    pub workflow_type: String,
    /// Workflow definition version selected by this plan.
    pub definition_version: WorkflowDefinitionVersion,
    /// Serialized compiled plan schema version.
    pub plan_schema_version: AgentCompiledPlanSchemaVersion,
    /// Deterministic fingerprint for the immutable compiled plan.
    pub plan_fingerprint: AgentCompiledPlanFingerprint,
    /// Entry nodes the scheduler may make runnable at run initialization.
    pub entry_node_ids: Vec<AgentCompiledNodeId>,
    /// Node definitions in the compiled graph.
    pub nodes: Vec<AgentCompiledPlanNode>,
    /// Directed edges between output and input ports.
    pub edges: Vec<AgentCompiledPlanEdge>,
    /// Optional source graph digest or artifact supplied by the application compiler.
    #[serde(default)]
    pub source_graph_ref: Option<ArtifactRef>,
    /// Optional compiled metadata artifact supplied by the application compiler.
    #[serde(default)]
    pub compiled_metadata_ref: Option<ArtifactRef>,
    /// Optional payload or schema artifacts used by this plan.
    #[serde(default)]
    pub payload_schema_refs: Vec<ArtifactRef>,
    /// Optional default retry policy artifact.
    #[serde(default)]
    pub default_retry_policy_ref: Option<ArtifactRef>,
    /// Optional default timeout policy artifact.
    #[serde(default)]
    pub default_timeout_policy_ref: Option<ArtifactRef>,
    /// Optional default approval policy artifact.
    #[serde(default)]
    pub default_approval_policy_ref: Option<ArtifactRef>,
    /// Optional default concurrency policy artifact.
    #[serde(default)]
    pub default_concurrency_policy_ref: Option<ArtifactRef>,
    /// Compatibility metadata used during rolling updates.
    #[serde(default)]
    pub compatibility: AgentCompiledPlanCompatibility,
    /// Bounded labels suitable for metrics when values are controlled.
    #[serde(default)]
    pub observability_labels: AgentAttributes,
}

impl AgentCompiledExecutionPlan {
    /// Creates a compiled execution plan with empty optional references.
    #[must_use]
    pub fn new(
        plan_id: AgentCompiledPlanId,
        workflow_id: AgentWorkflowId,
        workflow_type: impl Into<String>,
        definition_version: WorkflowDefinitionVersion,
        plan_schema_version: AgentCompiledPlanSchemaVersion,
        plan_fingerprint: AgentCompiledPlanFingerprint,
    ) -> Self {
        Self {
            plan_id,
            workflow_id,
            workflow_type: workflow_type.into(),
            definition_version,
            plan_schema_version,
            plan_fingerprint,
            entry_node_ids: Vec::new(),
            nodes: Vec::new(),
            edges: Vec::new(),
            source_graph_ref: None,
            compiled_metadata_ref: None,
            payload_schema_refs: Vec::new(),
            default_retry_policy_ref: None,
            default_timeout_policy_ref: None,
            default_approval_policy_ref: None,
            default_concurrency_policy_ref: None,
            compatibility: AgentCompiledPlanCompatibility::default(),
            observability_labels: BTreeMap::new(),
        }
    }

    /// Adds an entry node id.
    #[must_use]
    pub fn entry_node(mut self, node_id: impl Into<AgentCompiledNodeId>) -> Self {
        self.entry_node_ids.push(node_id.into());
        self
    }

    /// Adds a node definition.
    #[must_use]
    pub fn node(mut self, node: AgentCompiledPlanNode) -> Self {
        self.nodes.push(node);
        self
    }

    /// Adds a directed edge definition.
    #[must_use]
    pub fn edge(mut self, edge: AgentCompiledPlanEdge) -> Self {
        self.edges.push(edge);
        self
    }

    /// Adds a bounded observability label.
    #[must_use]
    pub fn observability_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.observability_labels.insert(key.into(), value.into());
        self
    }
}

/// Compatibility metadata for one compiled execution plan.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledPlanCompatibility {
    /// Minimum Rakka agent workflow runtime version expected by this plan.
    #[serde(default)]
    pub min_runtime_version: Option<String>,
    /// Maximum Rakka agent workflow runtime version expected by this plan.
    #[serde(default)]
    pub max_runtime_version: Option<String>,
    /// Optional named runtime capabilities required by this plan.
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    /// Bounded compatibility metadata.
    #[serde(default)]
    pub attributes: AgentAttributes,
}

impl AgentCompiledPlanCompatibility {
    /// Creates empty compatibility metadata.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the minimum runtime version.
    #[must_use]
    pub fn min_runtime_version(mut self, version: impl Into<String>) -> Self {
        self.min_runtime_version = Some(version.into());
        self
    }

    /// Adds a required runtime capability.
    #[must_use]
    pub fn required_capability(mut self, capability: impl Into<String>) -> Self {
        self.required_capabilities.push(capability.into());
        self
    }
}

/// One node in a compiled execution plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledPlanNode {
    /// Stable node id.
    pub node_id: AgentCompiledNodeId,
    /// Product-neutral runtime node kind.
    pub kind: AgentCompiledNodeKind,
    /// Declared input ports.
    #[serde(default)]
    pub input_ports: Vec<AgentCompiledPlanPort>,
    /// Declared output ports.
    #[serde(default)]
    pub output_ports: Vec<AgentCompiledPlanPort>,
    /// Optional diagnostic display name.
    #[serde(default)]
    pub display_name: Option<String>,
    /// Optional artifact reference for node configuration.
    #[serde(default)]
    pub config_ref: Option<ArtifactRef>,
    /// Optional node-specific retry policy artifact.
    #[serde(default)]
    pub retry_policy_ref: Option<ArtifactRef>,
    /// Optional node-specific timeout policy artifact.
    #[serde(default)]
    pub timeout_policy_ref: Option<ArtifactRef>,
    /// Optional node-specific concurrency policy artifact.
    #[serde(default)]
    pub concurrency_policy_ref: Option<ArtifactRef>,
    /// Optional bounded iterator policy for iterator nodes.
    #[serde(default)]
    pub iterator_policy: Option<AgentCompiledIteratorPolicy>,
    /// Optional logical runtime target for effect-producing nodes.
    #[serde(default)]
    pub target: Option<AgentCompiledNodeTarget>,
    /// Optional logical credential binding reference for effect-producing nodes.
    #[serde(default)]
    pub credential_binding_ref: Option<AgentCredentialBindingRef>,
    /// Bounded labels suitable for metrics when values are controlled.
    #[serde(default)]
    pub observability_labels: AgentAttributes,
}

impl AgentCompiledPlanNode {
    /// Creates a node with no ports or optional references.
    #[must_use]
    pub fn new(node_id: impl Into<AgentCompiledNodeId>, kind: AgentCompiledNodeKind) -> Self {
        Self {
            node_id: node_id.into(),
            kind,
            input_ports: Vec::new(),
            output_ports: Vec::new(),
            display_name: None,
            config_ref: None,
            retry_policy_ref: None,
            timeout_policy_ref: None,
            concurrency_policy_ref: None,
            iterator_policy: None,
            target: None,
            credential_binding_ref: None,
            observability_labels: BTreeMap::new(),
        }
    }

    /// Adds an input port.
    #[must_use]
    pub fn input_port(mut self, port: AgentCompiledPlanPort) -> Self {
        self.input_ports.push(port);
        self
    }

    /// Adds an output port.
    #[must_use]
    pub fn output_port(mut self, port: AgentCompiledPlanPort) -> Self {
        self.output_ports.push(port);
        self
    }

    /// Sets the logical target.
    #[must_use]
    pub fn target(mut self, target: AgentCompiledNodeTarget) -> Self {
        self.target = Some(target);
        self
    }

    /// Sets the logical credential binding reference.
    #[must_use]
    pub fn credential_binding_ref(mut self, binding: AgentCredentialBindingRef) -> Self {
        self.credential_binding_ref = Some(binding);
        self
    }

    /// Sets the bounded iterator policy.
    #[must_use]
    pub const fn iterator_policy(mut self, policy: AgentCompiledIteratorPolicy) -> Self {
        self.iterator_policy = Some(policy);
        self
    }

    /// Adds a bounded observability label.
    #[must_use]
    pub fn observability_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.observability_labels.insert(key.into(), value.into());
        self
    }
}

/// Bounded iterator policy for an explicit iterator node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledIteratorPolicy {
    /// Maximum iteration count allowed for this iterator node.
    pub max_iterations: u32,
}

impl AgentCompiledIteratorPolicy {
    /// Creates an iterator policy.
    #[must_use]
    pub const fn new(max_iterations: u32) -> Self {
        Self { max_iterations }
    }
}

/// Product-neutral categories of compiled runtime node behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentCompiledNodeKind {
    /// Workflow input boundary.
    Input,
    /// Local deterministic transformation.
    Transform,
    /// Branching decision.
    Branch,
    /// Fan-in join.
    Join,
    /// Explicit bounded iterator.
    Iterator,
    /// Model provider request.
    ModelCall,
    /// Tool adapter request.
    ToolCall,
    /// Process actor request.
    ProcessCall,
    /// HTTP request.
    HttpCall,
    /// gRPC request.
    GrpcCall,
    /// Stream publication.
    StreamPublish,
    /// Artifact write.
    ArtifactWrite,
    /// Human checkpoint.
    HumanCheckpoint,
    /// Durable timer wait.
    TimerWait,
    /// Child workflow command.
    ChildWorkflowCommand,
    /// Notification request.
    Notification,
    /// Durable audit event.
    AuditEvent,
    /// Terminal node.
    Terminal,
}

impl AgentCompiledNodeKind {
    /// Returns every compiled node kind supported by this runtime surface.
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::Input,
            Self::Transform,
            Self::Branch,
            Self::Join,
            Self::Iterator,
            Self::ModelCall,
            Self::ToolCall,
            Self::ProcessCall,
            Self::HttpCall,
            Self::GrpcCall,
            Self::StreamPublish,
            Self::ArtifactWrite,
            Self::HumanCheckpoint,
            Self::TimerWait,
            Self::ChildWorkflowCommand,
            Self::Notification,
            Self::AuditEvent,
            Self::Terminal,
        ]
    }

    /// Stable lowercase label for diagnostics and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Transform => "transform",
            Self::Branch => "branch",
            Self::Join => "join",
            Self::Iterator => "iterator",
            Self::ModelCall => "model-call",
            Self::ToolCall => "tool-call",
            Self::ProcessCall => "process-call",
            Self::HttpCall => "http-call",
            Self::GrpcCall => "grpc-call",
            Self::StreamPublish => "stream-publish",
            Self::ArtifactWrite => "artifact-write",
            Self::HumanCheckpoint => "human-checkpoint",
            Self::TimerWait => "timer-wait",
            Self::ChildWorkflowCommand => "child-workflow-command",
            Self::Notification => "notification",
            Self::AuditEvent => "audit-event",
            Self::Terminal => "terminal",
        }
    }

    /// Parses a stable lowercase node-kind label.
    #[must_use]
    pub fn from_label(value: &str) -> Option<Self> {
        Self::all()
            .iter()
            .copied()
            .find(|kind| kind.as_label() == value)
    }

    /// Returns a runtime descriptor for this node kind.
    #[must_use]
    pub fn descriptor(self) -> AgentCompiledNodeKindDescriptor {
        let mut descriptor = AgentCompiledNodeKindDescriptor::new(self);
        match self {
            Self::Input => {
                descriptor.input_port_policy = AgentCompiledPortPolicy::None;
                descriptor.output_port_policy = AgentCompiledPortPolicy::AtLeastOne;
            }
            Self::Transform => {
                descriptor.input_port_policy = AgentCompiledPortPolicy::AtLeastOne;
                descriptor.output_port_policy = AgentCompiledPortPolicy::AtLeastOne;
                descriptor.supports_config_ref = true;
                descriptor.supports_timeout_policy_ref = true;
            }
            Self::Branch => {
                descriptor.input_port_policy = AgentCompiledPortPolicy::AtLeastOne;
                descriptor.output_port_policy = AgentCompiledPortPolicy::AtLeastTwo;
                descriptor.supports_config_ref = true;
                descriptor.branch_semantics = true;
            }
            Self::Join => {
                descriptor.input_port_policy = AgentCompiledPortPolicy::AtLeastOne;
                descriptor.output_port_policy = AgentCompiledPortPolicy::AtLeastOne;
                descriptor.join_semantics = true;
            }
            Self::Iterator => {
                descriptor.input_port_policy = AgentCompiledPortPolicy::AtLeastOne;
                descriptor.output_port_policy = AgentCompiledPortPolicy::AtLeastOne;
                descriptor.supports_config_ref = true;
                descriptor.supports_timeout_policy_ref = true;
                descriptor.iterator_semantics = true;
            }
            Self::ModelCall => {
                descriptor.requires_target = true;
                descriptor.supports_credential_binding = true;
                descriptor.supports_config_ref = true;
                descriptor.supports_retry_policy_ref = true;
                descriptor.supports_timeout_policy_ref = true;
                descriptor.supports_concurrency_policy_ref = true;
            }
            Self::ToolCall
            | Self::ProcessCall
            | Self::HttpCall
            | Self::GrpcCall
            | Self::StreamPublish
            | Self::ArtifactWrite
            | Self::Notification
            | Self::ChildWorkflowCommand => {
                descriptor.requires_target = true;
                descriptor.supports_credential_binding = true;
                descriptor.supports_config_ref = true;
                descriptor.supports_retry_policy_ref = true;
                descriptor.supports_timeout_policy_ref = true;
                descriptor.supports_concurrency_policy_ref = true;
            }
            Self::HumanCheckpoint => {
                descriptor.supports_config_ref = true;
                descriptor.supports_timeout_policy_ref = true;
                descriptor.supports_approval_policy_ref = true;
            }
            Self::TimerWait => {
                descriptor.supports_config_ref = true;
                descriptor.supports_timeout_policy_ref = true;
            }
            Self::AuditEvent => {
                descriptor.supports_config_ref = true;
                descriptor.supports_retry_policy_ref = true;
            }
            Self::Terminal => {
                descriptor.input_port_policy = AgentCompiledPortPolicy::AtLeastOne;
                descriptor.output_port_policy = AgentCompiledPortPolicy::None;
            }
        }
        descriptor
    }
}

/// Runtime capability descriptor for one product-neutral compiled node kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledNodeKindDescriptor {
    /// Node kind described by this record.
    pub kind: AgentCompiledNodeKind,
    /// Stable lowercase kind label.
    pub label: String,
    /// Whether nodes of this kind must declare a logical target.
    pub requires_target: bool,
    /// Whether nodes of this kind may use logical credential binding refs.
    pub supports_credential_binding: bool,
    /// Whether nodes of this kind may use a config artifact ref.
    pub supports_config_ref: bool,
    /// Whether nodes of this kind may use a retry policy artifact ref.
    pub supports_retry_policy_ref: bool,
    /// Whether nodes of this kind may use a timeout policy artifact ref.
    pub supports_timeout_policy_ref: bool,
    /// Whether nodes of this kind may use a concurrency policy artifact ref.
    pub supports_concurrency_policy_ref: bool,
    /// Whether nodes of this kind may use an approval policy artifact ref.
    pub supports_approval_policy_ref: bool,
    /// Expected input port policy.
    pub input_port_policy: AgentCompiledPortPolicy,
    /// Expected output port policy.
    pub output_port_policy: AgentCompiledPortPolicy,
    /// Whether this node kind has branch semantics.
    pub branch_semantics: bool,
    /// Whether this node kind has join semantics.
    pub join_semantics: bool,
    /// Whether this node kind has bounded iterator semantics.
    pub iterator_semantics: bool,
    /// Required runtime capability, when applicable.
    pub required_capability: Option<String>,
    /// Required feature flag, when applicable.
    pub required_feature: Option<String>,
    /// Whether this kind is available in the current build/configuration.
    pub available: bool,
}

impl AgentCompiledNodeKindDescriptor {
    /// Creates a default descriptor for a supported node kind.
    #[must_use]
    pub fn new(kind: AgentCompiledNodeKind) -> Self {
        Self {
            kind,
            label: kind.as_label().to_string(),
            requires_target: false,
            supports_credential_binding: false,
            supports_config_ref: false,
            supports_retry_policy_ref: false,
            supports_timeout_policy_ref: false,
            supports_concurrency_policy_ref: false,
            supports_approval_policy_ref: false,
            input_port_policy: AgentCompiledPortPolicy::Any,
            output_port_policy: AgentCompiledPortPolicy::Any,
            branch_semantics: false,
            join_semantics: false,
            iterator_semantics: false,
            required_capability: Some(COMPILED_GRAPH_V1_CAPABILITY.to_string()),
            required_feature: None,
            available: true,
        }
    }
}

/// Port cardinality policy for compiled node kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentCompiledPortPolicy {
    /// Any number of ports is accepted.
    Any,
    /// No ports are expected.
    None,
    /// At least one port is expected.
    AtLeastOne,
    /// At least two ports are expected.
    AtLeastTwo,
}

impl AgentCompiledPortPolicy {
    /// Returns true when the port count satisfies this policy.
    #[must_use]
    pub const fn accepts(self, count: usize) -> bool {
        match self {
            Self::Any => true,
            Self::None => count == 0,
            Self::AtLeastOne => count >= 1,
            Self::AtLeastTwo => count >= 2,
        }
    }

    /// Stable lowercase label for diagnostics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::None => "none",
            Self::AtLeastOne => "at-least-one",
            Self::AtLeastTwo => "at-least-two",
        }
    }
}

/// Catalog of compiled runtime node capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledNodeKindCatalog {
    /// Runtime capabilities associated with this catalog.
    pub runtime_capabilities: AgentCompiledPlanRuntimeCapabilities,
    /// Supported node-kind descriptors.
    pub node_kinds: Vec<AgentCompiledNodeKindDescriptor>,
}

impl AgentCompiledNodeKindCatalog {
    /// Creates a catalog for the current default runtime capabilities.
    #[must_use]
    pub fn current() -> Self {
        Self::for_capabilities(AgentCompiledPlanRuntimeCapabilities::current())
    }

    /// Creates a catalog for explicit runtime capabilities.
    #[must_use]
    pub fn for_capabilities(capabilities: AgentCompiledPlanRuntimeCapabilities) -> Self {
        let node_kinds = AgentCompiledNodeKind::all()
            .iter()
            .copied()
            .map(|kind| {
                let mut descriptor = kind.descriptor();
                descriptor.available = descriptor
                    .required_capability
                    .as_deref()
                    .map_or(true, |required| capabilities.has_capability(required))
                    && descriptor
                        .required_feature
                        .as_deref()
                        .map_or(true, |required| capabilities.has_enabled_feature(required));
                descriptor
            })
            .collect();
        Self {
            runtime_capabilities: capabilities,
            node_kinds,
        }
    }

    /// Returns the descriptor for a node kind.
    #[must_use]
    pub fn descriptor(
        &self,
        kind: AgentCompiledNodeKind,
    ) -> Option<&AgentCompiledNodeKindDescriptor> {
        self.node_kinds
            .iter()
            .find(|descriptor| descriptor.kind == kind)
    }
}

impl Default for AgentCompiledNodeKindCatalog {
    fn default() -> Self {
        Self::current()
    }
}

/// Runtime capabilities available to compiled plan validation and scheduling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledPlanRuntimeCapabilities {
    /// Rakka agent workflow runtime version.
    pub runtime_version: String,
    /// Supported runtime capability labels.
    pub capabilities: Vec<String>,
    /// Enabled feature labels.
    pub enabled_features: Vec<String>,
}

impl AgentCompiledPlanRuntimeCapabilities {
    /// Creates runtime capabilities from explicit labels.
    #[must_use]
    pub fn new(runtime_version: impl Into<String>) -> Self {
        Self {
            runtime_version: runtime_version.into(),
            capabilities: Vec::new(),
            enabled_features: Vec::new(),
        }
    }

    /// Returns the current default runtime capabilities.
    #[must_use]
    pub fn current() -> Self {
        Self::new(DEFAULT_COMPILED_PLAN_RUNTIME_VERSION).capability(COMPILED_GRAPH_V1_CAPABILITY)
    }

    /// Adds a supported runtime capability.
    #[must_use]
    pub fn capability(mut self, capability: impl Into<String>) -> Self {
        self.capabilities.push(capability.into());
        self
    }

    /// Adds an enabled feature label.
    #[must_use]
    pub fn enabled_feature(mut self, feature: impl Into<String>) -> Self {
        self.enabled_features.push(feature.into());
        self
    }

    /// Returns true when a capability is available.
    #[must_use]
    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities
            .iter()
            .any(|available| available == capability)
    }

    /// Returns true when a feature is enabled.
    #[must_use]
    pub fn has_enabled_feature(&self, feature: &str) -> bool {
        self.enabled_features
            .iter()
            .any(|enabled| enabled == feature)
    }
}

/// Logical runtime target for an effect-producing node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledNodeTarget {
    /// Target category such as `model`, `tool`, `http`, or `grpc`.
    pub target_type: String,
    /// Stable logical target name.
    pub name: String,
    /// Optional route or logical endpoint.
    #[serde(default)]
    pub address: Option<String>,
    /// Bounded target metadata.
    #[serde(default)]
    pub attributes: AgentAttributes,
}

impl AgentCompiledNodeTarget {
    /// Creates a logical target descriptor.
    #[must_use]
    pub fn new(target_type: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            target_type: target_type.into(),
            name: name.into(),
            address: None,
            attributes: BTreeMap::new(),
        }
    }

    /// Sets a logical route or endpoint.
    #[must_use]
    pub fn address(mut self, address: impl Into<String>) -> Self {
        self.address = Some(address.into());
        self
    }

    /// Adds bounded target metadata.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// One input or output port declared by a compiled plan node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledPlanPort {
    /// Stable port id.
    pub port_id: AgentCompiledPortId,
    /// Port direction.
    pub direction: AgentCompiledPortDirection,
    /// Application-owned payload type name.
    pub payload_type: String,
    /// Whether this input must be satisfied before the node can run.
    #[serde(default = "default_true")]
    pub required: bool,
    /// Optional payload schema artifact reference.
    #[serde(default)]
    pub schema_ref: Option<ArtifactRef>,
    /// Bounded port metadata.
    #[serde(default)]
    pub attributes: AgentAttributes,
}

impl AgentCompiledPlanPort {
    /// Creates a required port.
    #[must_use]
    pub fn new(
        port_id: impl Into<AgentCompiledPortId>,
        direction: AgentCompiledPortDirection,
        payload_type: impl Into<String>,
    ) -> Self {
        Self {
            port_id: port_id.into(),
            direction,
            payload_type: payload_type.into(),
            required: true,
            schema_ref: None,
            attributes: BTreeMap::new(),
        }
    }

    /// Marks the port optional.
    #[must_use]
    pub const fn optional(mut self) -> Self {
        self.required = false;
        self
    }

    /// Adds bounded port metadata.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// Direction of a compiled plan port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentCompiledPortDirection {
    /// Input port.
    Input,
    /// Output port.
    Output,
}

impl AgentCompiledPortDirection {
    /// Stable lowercase label for diagnostics and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
        }
    }
}

/// One directed edge between two compiled plan ports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledPlanEdge {
    /// Stable edge id.
    pub edge_id: AgentCompiledEdgeId,
    /// Source node id.
    pub source_node_id: AgentCompiledNodeId,
    /// Source output port id.
    pub source_port_id: AgentCompiledPortId,
    /// Target node id.
    pub target_node_id: AgentCompiledNodeId,
    /// Target input port id.
    pub target_port_id: AgentCompiledPortId,
    /// Optional artifact reference for branch condition metadata.
    #[serde(default)]
    pub condition_ref: Option<ArtifactRef>,
    /// Optional merge behavior hint for joins.
    #[serde(default)]
    pub merge_behavior: Option<AgentCompiledEdgeMergeBehavior>,
    /// Bounded edge metadata.
    #[serde(default)]
    pub attributes: AgentAttributes,
}

impl AgentCompiledPlanEdge {
    /// Creates a directed edge between two node ports.
    #[must_use]
    pub fn new(
        edge_id: impl Into<AgentCompiledEdgeId>,
        source_node_id: impl Into<AgentCompiledNodeId>,
        source_port_id: impl Into<AgentCompiledPortId>,
        target_node_id: impl Into<AgentCompiledNodeId>,
        target_port_id: impl Into<AgentCompiledPortId>,
    ) -> Self {
        Self {
            edge_id: edge_id.into(),
            source_node_id: source_node_id.into(),
            source_port_id: source_port_id.into(),
            target_node_id: target_node_id.into(),
            target_port_id: target_port_id.into(),
            condition_ref: None,
            merge_behavior: None,
            attributes: BTreeMap::new(),
        }
    }

    /// Sets join merge behavior metadata.
    #[must_use]
    pub const fn merge_behavior(mut self, merge_behavior: AgentCompiledEdgeMergeBehavior) -> Self {
        self.merge_behavior = Some(merge_behavior);
        self
    }

    /// Adds bounded edge metadata.
    #[must_use]
    pub fn attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes.insert(key.into(), value.into());
        self
    }
}

/// Merge behavior used by downstream join nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentCompiledEdgeMergeBehavior {
    /// Downstream join waits for all required upstream edges.
    WaitForAll,
    /// Downstream join may continue after any selected upstream edge completes.
    WaitForAny,
}

impl AgentCompiledEdgeMergeBehavior {
    /// Stable lowercase label for diagnostics and metrics.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::WaitForAll => "wait-for-all",
            Self::WaitForAny => "wait-for-any",
        }
    }
}

const fn default_true() -> bool {
    true
}

/// Result type for compiled execution plan validation.
pub type AgentCompiledPlanValidationResult<T> = Result<T, AgentCompiledPlanValidationError>;

/// Validation failure for a compiled execution plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentCompiledPlanValidationError {
    /// A required field is empty or otherwise malformed.
    InvalidField {
        /// Stable field path or logical field name.
        field: String,
        /// Bounded diagnostic reason.
        reason: &'static str,
    },
    /// A required runtime capability is unavailable.
    MissingRuntimeCapability {
        /// Missing capability label.
        capability: String,
    },
    /// A node id appears more than once.
    DuplicateNodeId {
        /// Duplicate node id.
        node_id: AgentCompiledNodeId,
    },
    /// An edge id appears more than once.
    DuplicateEdgeId {
        /// Duplicate edge id.
        edge_id: AgentCompiledEdgeId,
    },
    /// A port id appears more than once on the same node.
    DuplicatePortId {
        /// Owning node id.
        node_id: AgentCompiledNodeId,
        /// Duplicate port id.
        port_id: AgentCompiledPortId,
    },
    /// An entry node id does not reference a declared node.
    UnknownEntryNode {
        /// Unknown entry node id.
        node_id: AgentCompiledNodeId,
    },
    /// An edge endpoint does not reference a declared node.
    UnknownEdgeNode {
        /// Edge with the invalid endpoint.
        edge_id: AgentCompiledEdgeId,
        /// Unknown node id.
        node_id: AgentCompiledNodeId,
        /// Endpoint role, such as `source` or `target`.
        role: &'static str,
    },
    /// An edge endpoint does not reference a declared port.
    UnknownEdgePort {
        /// Edge with the invalid endpoint.
        edge_id: AgentCompiledEdgeId,
        /// Node containing the missing port.
        node_id: AgentCompiledNodeId,
        /// Unknown port id.
        port_id: AgentCompiledPortId,
        /// Expected port direction.
        expected_direction: AgentCompiledPortDirection,
    },
    /// An edge endpoint references a port declared with the wrong direction.
    PortDirectionMismatch {
        /// Edge with the invalid endpoint.
        edge_id: AgentCompiledEdgeId,
        /// Node containing the mismatched port.
        node_id: AgentCompiledNodeId,
        /// Mismatched port id.
        port_id: AgentCompiledPortId,
        /// Direction required by the edge endpoint.
        expected_direction: AgentCompiledPortDirection,
        /// Direction declared by the port.
        actual_direction: AgentCompiledPortDirection,
    },
    /// A node kind is not present in the runtime capability catalog.
    UnsupportedNodeKind {
        /// Node using the unsupported kind.
        node_id: AgentCompiledNodeId,
        /// Unsupported node kind.
        kind: AgentCompiledNodeKind,
    },
    /// A node kind is known but unavailable in this runtime configuration.
    UnavailableNodeKind {
        /// Node using the unavailable kind.
        node_id: AgentCompiledNodeId,
        /// Unavailable node kind.
        kind: AgentCompiledNodeKind,
        /// Required capability label, when applicable.
        required_capability: Option<String>,
        /// Required feature label, when applicable.
        required_feature: Option<String>,
    },
    /// A node has a target even though its kind does not support targets.
    UnsupportedNodeTarget {
        /// Node with the unsupported target.
        node_id: AgentCompiledNodeId,
        /// Node kind.
        kind: AgentCompiledNodeKind,
    },
    /// A node kind requires a target but none was declared.
    MissingRequiredNodeTarget {
        /// Node missing the target.
        node_id: AgentCompiledNodeId,
        /// Node kind.
        kind: AgentCompiledNodeKind,
    },
    /// A node has a credential binding ref even though its kind does not support one.
    UnsupportedCredentialBinding {
        /// Node with the unsupported credential binding ref.
        node_id: AgentCompiledNodeId,
        /// Node kind.
        kind: AgentCompiledNodeKind,
    },
    /// A node has a policy reference unsupported by its kind.
    UnsupportedPolicyRef {
        /// Node with the unsupported policy reference.
        node_id: AgentCompiledNodeId,
        /// Node kind.
        kind: AgentCompiledNodeKind,
        /// Policy reference kind.
        policy: &'static str,
    },
    /// A node does not satisfy the catalog-declared port policy.
    InvalidPortPolicy {
        /// Node with the invalid port declaration.
        node_id: AgentCompiledNodeId,
        /// Node kind.
        kind: AgentCompiledNodeKind,
        /// Port direction being validated.
        direction: AgentCompiledPortDirection,
        /// Required policy.
        policy: AgentCompiledPortPolicy,
        /// Declared port count.
        count: usize,
    },
    /// An iterator node is missing a valid explicit bound.
    InvalidIteratorPolicy {
        /// Iterator node id.
        node_id: AgentCompiledNodeId,
        /// Bounded diagnostic reason.
        reason: &'static str,
    },
    /// A branch node declaration cannot be scheduled deterministically.
    InvalidBranchDeclaration {
        /// Branch node id.
        node_id: AgentCompiledNodeId,
        /// Bounded diagnostic reason.
        reason: &'static str,
    },
    /// A join node declaration cannot be scheduled deterministically.
    InvalidJoinDeclaration {
        /// Join node id.
        node_id: AgentCompiledNodeId,
        /// Bounded diagnostic reason.
        reason: &'static str,
    },
    /// A required input cannot be satisfied from an entry-reachable source.
    MissingRequiredInput {
        /// Node with the unsatisfied input.
        node_id: AgentCompiledNodeId,
        /// Required input port.
        port_id: AgentCompiledPortId,
    },
    /// No terminal node can be reached from the declared entry nodes.
    MissingReachableTerminal,
    /// An arbitrary graph cycle was detected.
    CycleDetected {
        /// A node participating in the detected cycle.
        node_id: AgentCompiledNodeId,
    },
    /// A bounded attribute or hot label contains unsafe, secret-like, or unbounded data.
    UnsafeAttribute {
        /// Logical scope for diagnostics.
        scope: String,
        /// Attribute key.
        key: String,
        /// Bounded diagnostic reason.
        reason: &'static str,
    },
    /// A logical credential binding ref looks like raw secret material.
    SecretLikeCredentialBindingRef {
        /// Node with the unsafe binding ref.
        node_id: AgentCompiledNodeId,
    },
}

impl AgentCompiledPlanValidationError {
    /// Stable validation error code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidField { .. } => "invalid_field",
            Self::MissingRuntimeCapability { .. } => "missing_runtime_capability",
            Self::DuplicateNodeId { .. } => "duplicate_node_id",
            Self::DuplicateEdgeId { .. } => "duplicate_edge_id",
            Self::DuplicatePortId { .. } => "duplicate_port_id",
            Self::UnknownEntryNode { .. } => "unknown_entry_node",
            Self::UnknownEdgeNode { .. } => "unknown_edge_node",
            Self::UnknownEdgePort { .. } => "unknown_edge_port",
            Self::PortDirectionMismatch { .. } => "port_direction_mismatch",
            Self::UnsupportedNodeKind { .. } => "unsupported_node_kind",
            Self::UnavailableNodeKind { .. } => "unavailable_node_kind",
            Self::UnsupportedNodeTarget { .. } => "unsupported_node_target",
            Self::MissingRequiredNodeTarget { .. } => "missing_required_node_target",
            Self::UnsupportedCredentialBinding { .. } => "unsupported_credential_binding",
            Self::UnsupportedPolicyRef { .. } => "unsupported_policy_ref",
            Self::InvalidPortPolicy { .. } => "invalid_port_policy",
            Self::InvalidIteratorPolicy { .. } => "invalid_iterator_policy",
            Self::InvalidBranchDeclaration { .. } => "invalid_branch_declaration",
            Self::InvalidJoinDeclaration { .. } => "invalid_join_declaration",
            Self::MissingRequiredInput { .. } => "missing_required_input",
            Self::MissingReachableTerminal => "missing_reachable_terminal",
            Self::CycleDetected { .. } => "cycle_detected",
            Self::UnsafeAttribute { .. } => "unsafe_attribute",
            Self::SecretLikeCredentialBindingRef { .. } => "secret_like_credential_binding_ref",
        }
    }
}

impl Display for AgentCompiledPlanValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidField { field, reason } => {
                write!(f, "invalid compiled plan field `{field}`: {reason}")
            }
            Self::MissingRuntimeCapability { capability } => {
                write!(
                    f,
                    "compiled plan requires unavailable runtime capability `{capability}`"
                )
            }
            Self::DuplicateNodeId { node_id } => {
                write!(f, "duplicate compiled plan node id `{node_id}`")
            }
            Self::DuplicateEdgeId { edge_id } => {
                write!(f, "duplicate compiled plan edge id `{edge_id}`")
            }
            Self::DuplicatePortId { node_id, port_id } => {
                write!(
                    f,
                    "duplicate compiled plan port id `{port_id}` on node `{node_id}`"
                )
            }
            Self::UnknownEntryNode { node_id } => {
                write!(f, "entry node `{node_id}` is not declared")
            }
            Self::UnknownEdgeNode {
                edge_id,
                node_id,
                role,
            } => {
                write!(
                    f,
                    "edge `{edge_id}` references unknown {role} node `{node_id}`"
                )
            }
            Self::UnknownEdgePort {
                edge_id,
                node_id,
                port_id,
                expected_direction,
            } => {
                write!(
                    f,
                    "edge `{edge_id}` references unknown {} port `{port_id}` on node `{node_id}`",
                    expected_direction.as_label()
                )
            }
            Self::PortDirectionMismatch {
                edge_id,
                node_id,
                port_id,
                expected_direction,
                actual_direction,
            } => {
                write!(
                    f,
                    "edge `{edge_id}` expected {} port `{port_id}` on node `{node_id}`, found {}",
                    expected_direction.as_label(),
                    actual_direction.as_label()
                )
            }
            Self::UnsupportedNodeKind { node_id, kind } => {
                write!(
                    f,
                    "node `{node_id}` uses unsupported node kind `{}`",
                    kind.as_label()
                )
            }
            Self::UnavailableNodeKind {
                node_id,
                kind,
                required_capability,
                required_feature,
            } => {
                write!(
                    f,
                    "node `{node_id}` uses unavailable node kind `{}`",
                    kind.as_label()
                )?;
                if let Some(capability) = required_capability {
                    write!(f, " requiring capability `{capability}`")?;
                }
                if let Some(feature) = required_feature {
                    write!(f, " requiring feature `{feature}`")?;
                }
                Ok(())
            }
            Self::UnsupportedNodeTarget { node_id, kind } => {
                write!(
                    f,
                    "node `{node_id}` of kind `{}` does not support a target",
                    kind.as_label()
                )
            }
            Self::MissingRequiredNodeTarget { node_id, kind } => {
                write!(
                    f,
                    "node `{node_id}` of kind `{}` requires a target",
                    kind.as_label()
                )
            }
            Self::UnsupportedCredentialBinding { node_id, kind } => {
                write!(
                    f,
                    "node `{node_id}` of kind `{}` does not support a credential binding ref",
                    kind.as_label()
                )
            }
            Self::UnsupportedPolicyRef {
                node_id,
                kind,
                policy,
            } => {
                write!(
                    f,
                    "node `{node_id}` of kind `{}` does not support `{policy}`",
                    kind.as_label()
                )
            }
            Self::InvalidPortPolicy {
                node_id,
                kind,
                direction,
                policy,
                count,
            } => {
                write!(
                    f,
                    "node `{node_id}` of kind `{}` has {count} {} ports, expected {}",
                    kind.as_label(),
                    direction.as_label(),
                    policy.as_label()
                )
            }
            Self::InvalidIteratorPolicy { node_id, reason } => {
                write!(f, "iterator node `{node_id}` has invalid policy: {reason}")
            }
            Self::InvalidBranchDeclaration { node_id, reason } => {
                write!(f, "branch node `{node_id}` is invalid: {reason}")
            }
            Self::InvalidJoinDeclaration { node_id, reason } => {
                write!(f, "join node `{node_id}` is invalid: {reason}")
            }
            Self::MissingRequiredInput { node_id, port_id } => {
                write!(
                    f,
                    "required input port `{port_id}` on node `{node_id}` is not reachable"
                )
            }
            Self::MissingReachableTerminal => {
                f.write_str("no terminal node is reachable from compiled plan entry nodes")
            }
            Self::CycleDetected { node_id } => {
                write!(
                    f,
                    "compiled plan contains an unsupported cycle at `{node_id}`"
                )
            }
            Self::UnsafeAttribute { scope, key, reason } => {
                write!(
                    f,
                    "unsafe compiled plan attribute `{key}` in `{scope}`: {reason}"
                )
            }
            Self::SecretLikeCredentialBindingRef { node_id } => {
                write!(
                    f,
                    "credential binding ref on node `{node_id}` looks like raw secret material"
                )
            }
        }
    }
}

impl Error for AgentCompiledPlanValidationError {}

/// Validates a compiled execution plan against the current runtime catalog.
pub fn validate_compiled_execution_plan(
    plan: &AgentCompiledExecutionPlan,
) -> AgentCompiledPlanValidationResult<()> {
    validate_compiled_execution_plan_with_catalog(plan, &AgentCompiledNodeKindCatalog::current())
}

/// Validates a compiled execution plan against an explicit runtime catalog.
pub fn validate_compiled_execution_plan_with_catalog(
    plan: &AgentCompiledExecutionPlan,
    catalog: &AgentCompiledNodeKindCatalog,
) -> AgentCompiledPlanValidationResult<()> {
    validate_required_text("plan_id", plan.plan_id.as_str())?;
    validate_required_text("workflow_id", plan.workflow_id.as_str())?;
    validate_required_text("workflow_type", &plan.workflow_type)?;
    validate_required_text("definition_version", plan.definition_version.as_str())?;
    validate_required_text("plan_fingerprint", plan.plan_fingerprint.as_str())?;
    if plan.plan_schema_version.get() == 0 {
        return Err(AgentCompiledPlanValidationError::InvalidField {
            field: "plan_schema_version".to_string(),
            reason: "schema version must be greater than zero",
        });
    }
    if plan.entry_node_ids.is_empty() {
        return Err(AgentCompiledPlanValidationError::InvalidField {
            field: "entry_node_ids".to_string(),
            reason: "at least one entry node is required",
        });
    }
    validate_attributes(
        "plan.observability_labels",
        &plan.observability_labels,
        true,
    )?;
    validate_attributes(
        "plan.compatibility.attributes",
        &plan.compatibility.attributes,
        false,
    )?;
    for required in &plan.compatibility.required_capabilities {
        validate_required_text("compatibility.required_capabilities[]", required)?;
        if !catalog.runtime_capabilities.has_capability(required) {
            return Err(AgentCompiledPlanValidationError::MissingRuntimeCapability {
                capability: required.clone(),
            });
        }
    }

    let mut nodes_by_id = BTreeMap::new();
    for node in &plan.nodes {
        validate_node_shape(node, catalog)?;
        if nodes_by_id.insert(node.node_id.clone(), node).is_some() {
            return Err(AgentCompiledPlanValidationError::DuplicateNodeId {
                node_id: node.node_id.clone(),
            });
        }
    }
    if nodes_by_id.is_empty() {
        return Err(AgentCompiledPlanValidationError::InvalidField {
            field: "nodes".to_string(),
            reason: "at least one node is required",
        });
    }

    let mut entry_ids = BTreeSet::new();
    for entry_node_id in &plan.entry_node_ids {
        validate_required_text("entry_node_ids[]", entry_node_id.as_str())?;
        if !nodes_by_id.contains_key(entry_node_id) {
            return Err(AgentCompiledPlanValidationError::UnknownEntryNode {
                node_id: entry_node_id.clone(),
            });
        }
        entry_ids.insert(entry_node_id.clone());
    }

    let mut edge_ids = BTreeSet::new();
    let mut adjacency = empty_adjacency(&nodes_by_id);
    let mut incoming_by_port = BTreeSet::new();
    let mut outgoing_by_port = BTreeSet::new();
    let mut incoming_edges_by_node: BTreeMap<AgentCompiledNodeId, Vec<&AgentCompiledPlanEdge>> =
        BTreeMap::new();

    for edge in &plan.edges {
        validate_edge_fields(edge)?;
        if !edge_ids.insert(edge.edge_id.clone()) {
            return Err(AgentCompiledPlanValidationError::DuplicateEdgeId {
                edge_id: edge.edge_id.clone(),
            });
        }
        validate_edge_endpoint(
            edge,
            &nodes_by_id,
            &edge.source_node_id,
            &edge.source_port_id,
            AgentCompiledPortDirection::Output,
            "source",
        )?;
        validate_edge_endpoint(
            edge,
            &nodes_by_id,
            &edge.target_node_id,
            &edge.target_port_id,
            AgentCompiledPortDirection::Input,
            "target",
        )?;

        adjacency
            .entry(edge.source_node_id.clone())
            .or_default()
            .push(edge.target_node_id.clone());
        incoming_by_port.insert((edge.target_node_id.clone(), edge.target_port_id.clone()));
        outgoing_by_port.insert((edge.source_node_id.clone(), edge.source_port_id.clone()));
        incoming_edges_by_node
            .entry(edge.target_node_id.clone())
            .or_default()
            .push(edge);
    }

    if let Some(node_id) = detect_cycle(&adjacency) {
        return Err(AgentCompiledPlanValidationError::CycleDetected { node_id });
    }

    let reachable = reachable_nodes(&plan.entry_node_ids, &adjacency);
    if !reachable.iter().any(|node_id| {
        nodes_by_id
            .get(node_id)
            .is_some_and(|node| node.kind == AgentCompiledNodeKind::Terminal)
    }) {
        return Err(AgentCompiledPlanValidationError::MissingReachableTerminal);
    }

    for node in nodes_by_id.values() {
        validate_reachable_required_inputs(node, &entry_ids, &reachable, &incoming_by_port)?;
        validate_branch_edges(node, &outgoing_by_port)?;
        validate_join_edges(node, &incoming_edges_by_node)?;
    }

    Ok(())
}

fn validate_required_text(
    field: impl Into<String>,
    value: &str,
) -> AgentCompiledPlanValidationResult<()> {
    if value.trim().is_empty() {
        return Err(AgentCompiledPlanValidationError::InvalidField {
            field: field.into(),
            reason: "value must not be empty",
        });
    }
    Ok(())
}

fn validate_node_shape(
    node: &AgentCompiledPlanNode,
    catalog: &AgentCompiledNodeKindCatalog,
) -> AgentCompiledPlanValidationResult<()> {
    validate_required_text("nodes[].node_id", node.node_id.as_str())?;
    validate_attributes(
        format!("nodes[{}].observability_labels", node.node_id),
        &node.observability_labels,
        true,
    )?;
    if let Some(target) = &node.target {
        validate_node_target(node, target)?;
    }
    if let Some(binding) = &node.credential_binding_ref {
        validate_required_text(
            format!("nodes[{}].credential_binding_ref", node.node_id),
            binding.as_str(),
        )?;
        if looks_like_secret_value(binding.as_str()) {
            return Err(
                AgentCompiledPlanValidationError::SecretLikeCredentialBindingRef {
                    node_id: node.node_id.clone(),
                },
            );
        }
    }

    let descriptor = catalog.descriptor(node.kind).ok_or_else(|| {
        AgentCompiledPlanValidationError::UnsupportedNodeKind {
            node_id: node.node_id.clone(),
            kind: node.kind,
        }
    })?;
    if !descriptor.available {
        return Err(AgentCompiledPlanValidationError::UnavailableNodeKind {
            node_id: node.node_id.clone(),
            kind: node.kind,
            required_capability: descriptor.required_capability.clone(),
            required_feature: descriptor.required_feature.clone(),
        });
    }
    if descriptor.requires_target && node.target.is_none() {
        return Err(
            AgentCompiledPlanValidationError::MissingRequiredNodeTarget {
                node_id: node.node_id.clone(),
                kind: node.kind,
            },
        );
    }
    if !descriptor.requires_target && node.target.is_some() {
        return Err(AgentCompiledPlanValidationError::UnsupportedNodeTarget {
            node_id: node.node_id.clone(),
            kind: node.kind,
        });
    }
    if !descriptor.supports_credential_binding && node.credential_binding_ref.is_some() {
        return Err(
            AgentCompiledPlanValidationError::UnsupportedCredentialBinding {
                node_id: node.node_id.clone(),
                kind: node.kind,
            },
        );
    }
    if !descriptor.supports_retry_policy_ref && node.retry_policy_ref.is_some() {
        return Err(AgentCompiledPlanValidationError::UnsupportedPolicyRef {
            node_id: node.node_id.clone(),
            kind: node.kind,
            policy: "retry_policy_ref",
        });
    }
    if !descriptor.supports_timeout_policy_ref && node.timeout_policy_ref.is_some() {
        return Err(AgentCompiledPlanValidationError::UnsupportedPolicyRef {
            node_id: node.node_id.clone(),
            kind: node.kind,
            policy: "timeout_policy_ref",
        });
    }
    if !descriptor.supports_concurrency_policy_ref && node.concurrency_policy_ref.is_some() {
        return Err(AgentCompiledPlanValidationError::UnsupportedPolicyRef {
            node_id: node.node_id.clone(),
            kind: node.kind,
            policy: "concurrency_policy_ref",
        });
    }
    if !descriptor.input_port_policy.accepts(node.input_ports.len()) {
        return Err(AgentCompiledPlanValidationError::InvalidPortPolicy {
            node_id: node.node_id.clone(),
            kind: node.kind,
            direction: AgentCompiledPortDirection::Input,
            policy: descriptor.input_port_policy,
            count: node.input_ports.len(),
        });
    }
    if !descriptor
        .output_port_policy
        .accepts(node.output_ports.len())
    {
        return Err(AgentCompiledPlanValidationError::InvalidPortPolicy {
            node_id: node.node_id.clone(),
            kind: node.kind,
            direction: AgentCompiledPortDirection::Output,
            policy: descriptor.output_port_policy,
            count: node.output_ports.len(),
        });
    }
    validate_ports(node)?;
    validate_iterator_policy(node, descriptor)
}

fn validate_node_target(
    node: &AgentCompiledPlanNode,
    target: &AgentCompiledNodeTarget,
) -> AgentCompiledPlanValidationResult<()> {
    validate_required_text(
        format!("nodes[{}].target.target_type", node.node_id),
        &target.target_type,
    )?;
    validate_required_text(format!("nodes[{}].target.name", node.node_id), &target.name)?;
    if let Some(address) = &target.address {
        validate_required_text(format!("nodes[{}].target.address", node.node_id), address)?;
    }
    validate_attributes(
        format!("nodes[{}].target.attributes", node.node_id),
        &target.attributes,
        false,
    )
}

fn validate_ports(node: &AgentCompiledPlanNode) -> AgentCompiledPlanValidationResult<()> {
    let mut port_ids = BTreeSet::new();
    for port in &node.input_ports {
        validate_port(node, port, AgentCompiledPortDirection::Input)?;
        if !port_ids.insert(port.port_id.clone()) {
            return Err(AgentCompiledPlanValidationError::DuplicatePortId {
                node_id: node.node_id.clone(),
                port_id: port.port_id.clone(),
            });
        }
    }
    for port in &node.output_ports {
        validate_port(node, port, AgentCompiledPortDirection::Output)?;
        if !port_ids.insert(port.port_id.clone()) {
            return Err(AgentCompiledPlanValidationError::DuplicatePortId {
                node_id: node.node_id.clone(),
                port_id: port.port_id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_port(
    node: &AgentCompiledPlanNode,
    port: &AgentCompiledPlanPort,
    expected_direction: AgentCompiledPortDirection,
) -> AgentCompiledPlanValidationResult<()> {
    validate_required_text(
        format!("nodes[{}].ports[].port_id", node.node_id),
        port.port_id.as_str(),
    )?;
    validate_required_text(
        format!(
            "nodes[{}].ports[{}].payload_type",
            node.node_id, port.port_id
        ),
        &port.payload_type,
    )?;
    if port.direction != expected_direction {
        return Err(AgentCompiledPlanValidationError::PortDirectionMismatch {
            edge_id: AgentCompiledEdgeId::new("<node-port-declaration>"),
            node_id: node.node_id.clone(),
            port_id: port.port_id.clone(),
            expected_direction,
            actual_direction: port.direction,
        });
    }
    validate_attributes(
        format!("nodes[{}].ports[{}].attributes", node.node_id, port.port_id),
        &port.attributes,
        false,
    )
}

fn validate_iterator_policy(
    node: &AgentCompiledPlanNode,
    descriptor: &AgentCompiledNodeKindDescriptor,
) -> AgentCompiledPlanValidationResult<()> {
    if descriptor.iterator_semantics {
        let Some(policy) = node.iterator_policy else {
            return Err(AgentCompiledPlanValidationError::InvalidIteratorPolicy {
                node_id: node.node_id.clone(),
                reason: "iterator nodes require an explicit iterator policy",
            });
        };
        if policy.max_iterations == 0 {
            return Err(AgentCompiledPlanValidationError::InvalidIteratorPolicy {
                node_id: node.node_id.clone(),
                reason: "max_iterations must be greater than zero",
            });
        }
    } else if node.iterator_policy.is_some() {
        return Err(AgentCompiledPlanValidationError::InvalidIteratorPolicy {
            node_id: node.node_id.clone(),
            reason: "only iterator nodes may declare iterator policy",
        });
    }
    Ok(())
}

fn validate_edge_fields(edge: &AgentCompiledPlanEdge) -> AgentCompiledPlanValidationResult<()> {
    validate_required_text("edges[].edge_id", edge.edge_id.as_str())?;
    validate_required_text(
        format!("edges[{}].source_node_id", edge.edge_id),
        edge.source_node_id.as_str(),
    )?;
    validate_required_text(
        format!("edges[{}].source_port_id", edge.edge_id),
        edge.source_port_id.as_str(),
    )?;
    validate_required_text(
        format!("edges[{}].target_node_id", edge.edge_id),
        edge.target_node_id.as_str(),
    )?;
    validate_required_text(
        format!("edges[{}].target_port_id", edge.edge_id),
        edge.target_port_id.as_str(),
    )?;
    validate_attributes(
        format!("edges[{}].attributes", edge.edge_id),
        &edge.attributes,
        false,
    )
}

fn validate_edge_endpoint(
    edge: &AgentCompiledPlanEdge,
    nodes_by_id: &BTreeMap<AgentCompiledNodeId, &AgentCompiledPlanNode>,
    node_id: &AgentCompiledNodeId,
    port_id: &AgentCompiledPortId,
    expected_direction: AgentCompiledPortDirection,
    role: &'static str,
) -> AgentCompiledPlanValidationResult<()> {
    let node = nodes_by_id.get(node_id).ok_or_else(|| {
        AgentCompiledPlanValidationError::UnknownEdgeNode {
            edge_id: edge.edge_id.clone(),
            node_id: node_id.clone(),
            role,
        }
    })?;
    if find_port(node, port_id, expected_direction).is_some() {
        return Ok(());
    }
    if let Some(actual_direction) = find_port_any_direction(node, port_id) {
        return Err(AgentCompiledPlanValidationError::PortDirectionMismatch {
            edge_id: edge.edge_id.clone(),
            node_id: node_id.clone(),
            port_id: port_id.clone(),
            expected_direction,
            actual_direction,
        });
    }
    Err(AgentCompiledPlanValidationError::UnknownEdgePort {
        edge_id: edge.edge_id.clone(),
        node_id: node_id.clone(),
        port_id: port_id.clone(),
        expected_direction,
    })
}

fn find_port<'a>(
    node: &'a AgentCompiledPlanNode,
    port_id: &AgentCompiledPortId,
    direction: AgentCompiledPortDirection,
) -> Option<&'a AgentCompiledPlanPort> {
    match direction {
        AgentCompiledPortDirection::Input => node
            .input_ports
            .iter()
            .find(|port| port.port_id == *port_id),
        AgentCompiledPortDirection::Output => node
            .output_ports
            .iter()
            .find(|port| port.port_id == *port_id),
    }
}

fn find_port_any_direction(
    node: &AgentCompiledPlanNode,
    port_id: &AgentCompiledPortId,
) -> Option<AgentCompiledPortDirection> {
    node.input_ports
        .iter()
        .find(|port| port.port_id == *port_id)
        .map(|port| port.direction)
        .or_else(|| {
            node.output_ports
                .iter()
                .find(|port| port.port_id == *port_id)
                .map(|port| port.direction)
        })
}

fn empty_adjacency(
    nodes_by_id: &BTreeMap<AgentCompiledNodeId, &AgentCompiledPlanNode>,
) -> BTreeMap<AgentCompiledNodeId, Vec<AgentCompiledNodeId>> {
    nodes_by_id
        .keys()
        .cloned()
        .map(|node_id| (node_id, Vec::new()))
        .collect()
}

fn detect_cycle(
    adjacency: &BTreeMap<AgentCompiledNodeId, Vec<AgentCompiledNodeId>>,
) -> Option<AgentCompiledNodeId> {
    // Iterative depth-first search with an explicit work stack so deeply nested
    // compiled plans cannot overflow the call stack during validation. `visiting`
    // holds the nodes on the current DFS path; an edge back into that set is a
    // cycle. This preserves the recursive version's result — the returned node is
    // the first on-path node a back-edge targets, in deterministic (sorted root,
    // edge-declaration) order — while running in heap-bounded space.
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for root in adjacency.keys() {
        if visited.contains(root) {
            continue;
        }
        visiting.insert(root.clone());
        let mut stack: Vec<(&AgentCompiledNodeId, usize)> = vec![(root, 0)];
        while let Some((node_id, child_index)) = stack.last().copied() {
            let children = adjacency.get(node_id).map(Vec::as_slice).unwrap_or(&[]);
            if let Some(next) = children.get(child_index) {
                if let Some(top) = stack.last_mut() {
                    top.1 = child_index + 1;
                }
                if visiting.contains(next) {
                    return Some(next.clone());
                }
                if !visited.contains(next) {
                    visiting.insert(next.clone());
                    stack.push((next, 0));
                }
            } else {
                visiting.remove(node_id);
                visited.insert(node_id.clone());
                stack.pop();
            }
        }
    }
    None
}

fn reachable_nodes(
    entry_node_ids: &[AgentCompiledNodeId],
    adjacency: &BTreeMap<AgentCompiledNodeId, Vec<AgentCompiledNodeId>>,
) -> BTreeSet<AgentCompiledNodeId> {
    let mut reachable = BTreeSet::new();
    let mut stack: Vec<_> = entry_node_ids.iter().cloned().rev().collect();
    while let Some(node_id) = stack.pop() {
        if !reachable.insert(node_id.clone()) {
            continue;
        }
        if let Some(next_node_ids) = adjacency.get(&node_id) {
            for next_node_id in next_node_ids.iter().rev() {
                stack.push(next_node_id.clone());
            }
        }
    }
    reachable
}

fn validate_reachable_required_inputs(
    node: &AgentCompiledPlanNode,
    entry_ids: &BTreeSet<AgentCompiledNodeId>,
    reachable: &BTreeSet<AgentCompiledNodeId>,
    incoming_by_port: &BTreeSet<(AgentCompiledNodeId, AgentCompiledPortId)>,
) -> AgentCompiledPlanValidationResult<()> {
    if entry_ids.contains(&node.node_id) {
        return Ok(());
    }
    for port in node.input_ports.iter().filter(|port| port.required) {
        if !reachable.contains(&node.node_id)
            || !incoming_by_port.contains(&(node.node_id.clone(), port.port_id.clone()))
        {
            return Err(AgentCompiledPlanValidationError::MissingRequiredInput {
                node_id: node.node_id.clone(),
                port_id: port.port_id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_branch_edges(
    node: &AgentCompiledPlanNode,
    outgoing_by_port: &BTreeSet<(AgentCompiledNodeId, AgentCompiledPortId)>,
) -> AgentCompiledPlanValidationResult<()> {
    if node.kind != AgentCompiledNodeKind::Branch {
        return Ok(());
    }
    let connected_output_count = node
        .output_ports
        .iter()
        .filter(|port| outgoing_by_port.contains(&(node.node_id.clone(), port.port_id.clone())))
        .count();
    if connected_output_count < 2 {
        return Err(AgentCompiledPlanValidationError::InvalidBranchDeclaration {
            node_id: node.node_id.clone(),
            reason: "branch nodes require at least two connected output paths",
        });
    }
    Ok(())
}

fn validate_join_edges(
    node: &AgentCompiledPlanNode,
    incoming_edges_by_node: &BTreeMap<AgentCompiledNodeId, Vec<&AgentCompiledPlanEdge>>,
) -> AgentCompiledPlanValidationResult<()> {
    if node.kind != AgentCompiledNodeKind::Join {
        return Ok(());
    }
    let incoming_edges = incoming_edges_by_node.get(&node.node_id).ok_or_else(|| {
        AgentCompiledPlanValidationError::InvalidJoinDeclaration {
            node_id: node.node_id.clone(),
            reason: "join nodes require at least one incoming edge",
        }
    })?;
    if incoming_edges
        .iter()
        .any(|edge| edge.merge_behavior.is_none())
    {
        return Err(AgentCompiledPlanValidationError::InvalidJoinDeclaration {
            node_id: node.node_id.clone(),
            reason: "join incoming edges must declare wait-for-all or wait-for-any behavior",
        });
    }
    Ok(())
}

fn validate_attributes(
    scope: impl Into<String>,
    attributes: &AgentAttributes,
    hot_labels: bool,
) -> AgentCompiledPlanValidationResult<()> {
    let scope = scope.into();
    for (key, value) in attributes {
        if key.trim().is_empty() {
            return Err(AgentCompiledPlanValidationError::UnsafeAttribute {
                scope,
                key: key.clone(),
                reason: "attribute keys must not be empty",
            });
        }
        if key.len() > AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES {
            return Err(AgentCompiledPlanValidationError::UnsafeAttribute {
                scope,
                key: key.clone(),
                reason: "attribute keys must be bounded",
            });
        }
        if value.len() > AGENT_METRIC_ATTRIBUTE_VALUE_MAX_BYTES {
            return Err(AgentCompiledPlanValidationError::UnsafeAttribute {
                scope,
                key: key.clone(),
                reason: "attribute values must be bounded",
            });
        }
        if label_value_contains_line_break(value) {
            return Err(AgentCompiledPlanValidationError::UnsafeAttribute {
                scope,
                key: key.clone(),
                reason: "attribute values must be single-line bounded labels",
            });
        }
        if is_sensitive_attribute_key(key) || looks_like_secret_value(value) {
            return Err(AgentCompiledPlanValidationError::UnsafeAttribute {
                scope,
                key: key.clone(),
                reason: "attributes must not contain credential or secret material",
            });
        }
        if hot_labels
            && (FORBIDDEN_HOT_METRIC_FIELDS.contains(&key.as_str())
                || looks_like_credential_binding_ref(value))
        {
            return Err(AgentCompiledPlanValidationError::UnsafeAttribute {
                scope,
                key: key.clone(),
                reason: "hot labels must not contain ids or credential binding refs",
            });
        }
    }
    Ok(())
}

fn is_sensitive_attribute_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase();
    normalized.contains("secret")
        || normalized.contains("token")
        || normalized.contains("password")
        || normalized.contains("api_key")
        || normalized.contains("apikey")
        || normalized.contains("authorization")
        || normalized.contains("credential")
}

fn looks_like_secret_value(value: &str) -> bool {
    let trimmed = value.trim();
    let lower = trimmed.to_ascii_lowercase();
    trimmed.starts_with("sk-")
        || trimmed.starts_with("xoxb-")
        || trimmed.starts_with("ghp_")
        || trimmed.starts_with("github_pat_")
        || trimmed.starts_with("AKIA")
        || trimmed.starts_with("-----BEGIN ")
        || lower.starts_with("bearer ")
        || lower.starts_with("basic ")
}

fn looks_like_credential_binding_ref(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase();
    normalized.contains("credential_binding") || normalized.contains("cred_binding")
}
