//! Product-neutral compiled execution plan contracts.
//!
//! Application backends compile editor-owned workflow DSLs into these durable
//! runtime contracts. Rakka interprets the compiled plan; it does not own the
//! editor DSL, UI layout, credential storage, trigger registration, or product
//! policy.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::{AgentAttributes, AgentWorkflowId, ArtifactRef, WorkflowDefinitionVersion};

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
    pub source_graph_ref: Option<ArtifactRef>,
    /// Optional compiled metadata artifact supplied by the application compiler.
    pub compiled_metadata_ref: Option<ArtifactRef>,
    /// Optional payload or schema artifacts used by this plan.
    pub payload_schema_refs: Vec<ArtifactRef>,
    /// Optional default retry policy artifact.
    pub default_retry_policy_ref: Option<ArtifactRef>,
    /// Optional default timeout policy artifact.
    pub default_timeout_policy_ref: Option<ArtifactRef>,
    /// Optional default approval policy artifact.
    pub default_approval_policy_ref: Option<ArtifactRef>,
    /// Optional default concurrency policy artifact.
    pub default_concurrency_policy_ref: Option<ArtifactRef>,
    /// Compatibility metadata used during rolling updates.
    pub compatibility: AgentCompiledPlanCompatibility,
    /// Bounded labels suitable for metrics when values are controlled.
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
    pub min_runtime_version: Option<String>,
    /// Maximum Rakka agent workflow runtime version expected by this plan.
    pub max_runtime_version: Option<String>,
    /// Optional named runtime capabilities required by this plan.
    pub required_capabilities: Vec<String>,
    /// Bounded compatibility metadata.
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
    pub input_ports: Vec<AgentCompiledPlanPort>,
    /// Declared output ports.
    pub output_ports: Vec<AgentCompiledPlanPort>,
    /// Optional diagnostic display name.
    pub display_name: Option<String>,
    /// Optional artifact reference for node configuration.
    pub config_ref: Option<ArtifactRef>,
    /// Optional node-specific retry policy artifact.
    pub retry_policy_ref: Option<ArtifactRef>,
    /// Optional node-specific timeout policy artifact.
    pub timeout_policy_ref: Option<ArtifactRef>,
    /// Optional node-specific concurrency policy artifact.
    pub concurrency_policy_ref: Option<ArtifactRef>,
    /// Optional logical runtime target for effect-producing nodes.
    pub target: Option<AgentCompiledNodeTarget>,
    /// Optional logical credential binding reference for effect-producing nodes.
    pub credential_binding_ref: Option<AgentCredentialBindingRef>,
    /// Bounded labels suitable for metrics when values are controlled.
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

    /// Adds a bounded observability label.
    #[must_use]
    pub fn observability_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.observability_labels.insert(key.into(), value.into());
        self
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
}

/// Logical runtime target for an effect-producing node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompiledNodeTarget {
    /// Target category such as `model`, `tool`, `http`, or `grpc`.
    pub target_type: String,
    /// Stable logical target name.
    pub name: String,
    /// Optional route or logical endpoint.
    pub address: Option<String>,
    /// Bounded target metadata.
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
    pub required: bool,
    /// Optional payload schema artifact reference.
    pub schema_ref: Option<ArtifactRef>,
    /// Bounded port metadata.
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
    pub condition_ref: Option<ArtifactRef>,
    /// Optional merge behavior hint for joins.
    pub merge_behavior: Option<AgentCompiledEdgeMergeBehavior>,
    /// Bounded edge metadata.
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
