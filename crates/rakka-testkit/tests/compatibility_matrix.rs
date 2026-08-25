//! V1 compatibility matrix and rolling-update hardening tests.

use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message as ProstMessage;
use rakka_cluster::{
    ClusterError, ClusterMembership, ClusterNode, ClusterProtocol, DiscoverySnapshot,
    MembershipConfig, NodeAddress, NodeId,
};
use rakka_core::{
    InMemoryMetricsRecorder, MetricKind, METRIC_K8S_COMPATIBILITY, METRIC_K8S_READINESS,
};
use rakka_grpc::{V1_GRPC_GENERATED_API_VERSION, V1_GRPC_PROTOBUF_COMPATIBILITY};
use rakka_http::{V1_HTTP_API_COMPATIBILITY, V1_HTTP_API_VERSION};
use rakka_k8s::KubernetesNodeHealth;
use rakka_remote::{
    EncodedPayload, InMemoryRemoteTransport, RemoteEndpoint, RemoteEnvelopeMetadata,
    SerializationRegistry, TcpRemoteHandshake,
};
use rakka_sharding::{
    ClusterShardingError, ClusterShardingRuntime, EntityDeliveryFailure, EntityId, EntityRoute,
    EntityTellError, EntityType, RemoteEntityInbound, RemoteEntityRoute,
    RemoteTransportEntityOutbound, RoutedEntityMessage, ShardCoordinator, ShardRegion,
    ShardingConfig,
};
use rakka_testkit::compatibility::{
    v1_compatibility_case_kinds, v1_compatibility_dimensions, CompatibilityCaseKind,
    CompatibilityDimension, V1CompatibilityFixture,
};
use rakka_testkit::{assert_metric_attribute, expect_metric_observation};

const MANIFEST: &str = include_str!("../../../examples/kubernetes/rakka-node.yaml");
const COMPAT_MESSAGE_TYPE: &str = "rakka.compat.Command";

#[derive(Clone, PartialEq, prost::Message)]
struct CompatCommand {
    #[prost(string, tag = "1")]
    action: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeliveredMessage {
    entity_id: String,
    action: String,
    owner: NodeId,
}

#[derive(Debug, Clone)]
struct RecordingLocalRoute {
    local_node_id: NodeId,
    delivered: Arc<Mutex<Vec<DeliveredMessage>>>,
}

impl RecordingLocalRoute {
    fn new(local_node_id: NodeId, delivered: Arc<Mutex<Vec<DeliveredMessage>>>) -> Self {
        Self {
            local_node_id,
            delivered,
        }
    }
}

impl EntityRoute<CompatCommand> for RecordingLocalRoute {
    fn deliver(
        &self,
        message: RoutedEntityMessage<CompatCommand>,
    ) -> Result<(), EntityTellError<CompatCommand>> {
        if message.owner() != &self.local_node_id {
            let owner = message.owner().clone();
            return Err(EntityTellError::Delivery {
                message: message.into_message(),
                failure: EntityDeliveryFailure::NotLocal { owner },
            });
        }

        self.delivered
            .lock()
            .expect("delivered messages mutex poisoned")
            .push(DeliveredMessage {
                entity_id: message.entity_id().as_str().to_string(),
                action: message.message().action.clone(),
                owner: message.owner().clone(),
            });
        Ok(())
    }

    fn local_node_id(&self) -> Option<&NodeId> {
        Some(&self.local_node_id)
    }
}

#[test]
fn v1_fixture_defines_required_dimensions_and_cases() {
    let fixture = V1CompatibilityFixture::new();

    for expected in [
        CompatibilityDimension::CrateVersion,
        CompatibilityDimension::ClusterProtocol,
        CompatibilityDimension::RemoteEnvelope,
        CompatibilityDimension::MessageSchema,
        CompatibilityDimension::KubernetesManifest,
        CompatibilityDimension::GeneratedApi,
    ] {
        assert!(
            v1_compatibility_dimensions().contains(&expected),
            "missing dimension {}",
            expected.as_str()
        );
    }

    for expected in [
        CompatibilityCaseKind::Current,
        CompatibilityCaseKind::Next,
        CompatibilityCaseKind::IncompatibleOld,
        CompatibilityCaseKind::IncompatibleNew,
        CompatibilityCaseKind::AdditiveSchema,
        CompatibilityCaseKind::ExactSchema,
    ] {
        assert!(
            v1_compatibility_case_kinds().contains(&expected),
            "missing case {}",
            expected.as_str()
        );
    }

    assert!(!fixture.crate_version().is_empty());
    assert_eq!(fixture.manifest_version(), "1.0");
    assert_eq!(fixture.generated_api_version(), "1.0");
    assert_eq!(fixture.current_protocol_version().to_string(), "1.0");
    assert_eq!(fixture.next_protocol_version().to_string(), "1.1");
    assert_eq!(fixture.current_schema_version(), 2);
    assert_eq!(fixture.envelope_version(), 1);
}

#[test]
fn protocol_and_handshake_matrix_accepts_only_n_to_n_plus_one() {
    let fixture = V1CompatibilityFixture::new();
    let local = node_with_protocol("compat-local", "n", 2552, fixture.current_protocol());

    for case in fixture.protocol_cases() {
        let remote = node_with_protocol(case.kind().as_str(), "remote", 2553, case.protocol());
        let handshake = TcpRemoteHandshake::new(
            remote.id().clone(),
            case.protocol(),
            fixture.envelope_version(),
            [format!("case:{}", case.kind().as_str())],
        );

        assert_eq!(handshake.protocol(), case.protocol());
        assert_eq!(handshake.envelope_version(), fixture.envelope_version());
        assert_eq!(
            fixture
                .current_protocol()
                .is_compatible_with(handshake.protocol()),
            case.expected_compatible(),
            "handshake case {}",
            case.kind().as_str()
        );

        let mut membership = ClusterMembership::new(local.clone(), membership_config());
        let result = membership.record_discovery(DiscoverySnapshot::new(
            "compatibility-matrix",
            1,
            [local.clone(), remote.clone()],
        ));

        assert_eq!(
            result.is_ok(),
            case.expected_compatible(),
            "membership case {}",
            case.kind().as_str()
        );

        if let Err(error) = result {
            assert!(
                matches!(error, ClusterError::IncompatibleNode { .. }),
                "expected incompatible-node error, got {error:?}"
            );
            assert_readiness_reports_compatibility_failure(local.id().clone(), error.to_string());
        }
    }
}

#[test]
fn remote_schema_matrix_accepts_additive_window_and_rejects_exact_incompatible_versions() {
    let fixture = V1CompatibilityFixture::new();
    let payload = encode_command("add-apple");
    let additive = compatibility_registry(fixture.additive_schema_policy(), &fixture);

    for case in fixture.schema_cases() {
        let encoded = EncodedPayload::new(
            RemoteEnvelopeMetadata::protobuf(COMPAT_MESSAGE_TYPE, case.schema_version()),
            payload.clone(),
        );
        let result = additive.decode::<CompatCommand>(&encoded);

        assert_eq!(
            result.is_ok(),
            case.expected_supported(),
            "schema case {} version {}",
            case.kind().as_str(),
            case.schema_version()
        );
    }

    let exact = compatibility_registry(fixture.exact_schema_policy(), &fixture);
    let accepted = EncodedPayload::new(
        RemoteEnvelopeMetadata::protobuf(COMPAT_MESSAGE_TYPE, fixture.current_schema_version()),
        payload.clone(),
    );
    let additive_old = EncodedPayload::new(
        RemoteEnvelopeMetadata::protobuf(
            COMPAT_MESSAGE_TYPE,
            fixture.current_schema_version().saturating_sub(1),
        ),
        payload,
    );

    assert!(exact.decode::<CompatCommand>(&accepted).is_ok());
    assert!(
        exact.decode::<CompatCommand>(&additive_old).is_err(),
        "exact schema policy must not accept the additive rolling-window alias"
    );
}

#[test]
fn mixed_n_to_n_plus_one_cluster_routes_supported_remote_message() {
    let fixture = V1CompatibilityFixture::new();
    let local = node_with_protocol("compat-node-a", "n", 2552, fixture.current_protocol());
    let remote = node_with_protocol("compat-node-b", "n-plus-one", 2553, fixture.next_protocol());
    let entity_type = EntityType::new("CompatCart");
    let config = ShardingConfig::new(8).expect("valid sharding config");
    let registry = compatibility_registry(fixture.additive_schema_policy(), &fixture);
    let delivered_a = Arc::new(Mutex::new(Vec::new()));
    let delivered_b = Arc::new(Mutex::new(Vec::new()));
    let transport = InMemoryRemoteTransport::new();

    let mut runtime = ClusterShardingRuntime::new(membership_with_up_nodes(
        local.clone(),
        [local.clone(), remote.clone()],
    ));
    let local_route_a = RecordingLocalRoute::new(local.id().clone(), delivered_a.clone());
    let remote_route_a = RemoteEntityRoute::new(
        local_route_a,
        registry.clone(),
        RemoteTransportEntityOutbound::new(transport.clone()),
    )
    .with_source(local.id().to_string());
    let region_a = ShardRegion::new(entity_type.clone(), config.clone(), remote_route_a);
    let local_route_b = RecordingLocalRoute::new(remote.id().clone(), delivered_b.clone());
    let region_b = ShardRegion::new(entity_type.clone(), config, local_route_b);

    runtime
        .register_region(region_a.clone())
        .expect("local region should register");
    runtime
        .register_region(region_b.clone())
        .expect("remote owner-cache region should register");

    let endpoint_b = RemoteEndpoint::new(remote.id().clone());
    endpoint_b
        .register_entity_handler(
            entity_type.as_str(),
            RemoteEntityInbound::new(region_b, registry.clone()),
        )
        .expect("remote endpoint handler should register");
    transport
        .register_endpoint(endpoint_b)
        .expect("remote endpoint should register");

    let coordinator = runtime
        .coordinator(&entity_type)
        .expect("coordinator should exist");
    let entity_id = entity_owned_by(coordinator, remote.id().logical_id());
    let entity = region_a.entity_ref(entity_id.as_str());

    region_a
        .tell(
            &entity,
            CompatCommand {
                action: "add-apple".to_string(),
            },
        )
        .expect("N node should route N+1-owned entity through remote envelope");

    assert!(
        delivered_a
            .lock()
            .expect("delivered messages mutex poisoned")
            .is_empty(),
        "node A should not receive a node B-owned entity message locally"
    );
    assert_eq!(
        delivered_b
            .lock()
            .expect("delivered messages mutex poisoned")
            .as_slice(),
        [DeliveredMessage {
            entity_id: entity_id.as_str().to_string(),
            action: "add-apple".to_string(),
            owner: remote.id().clone(),
        }]
    );
}

#[test]
fn incompatible_node_fails_readiness_and_does_not_acquire_shard_ownership() {
    let fixture = V1CompatibilityFixture::new();
    let local = node_with_protocol("compat-node-a", "n", 2552, fixture.current_protocol());
    let incompatible_protocol = fixture
        .protocol_cases()
        .into_iter()
        .find(|case| case.kind() == CompatibilityCaseKind::IncompatibleNew)
        .expect("fixture should include incompatible-new protocol")
        .protocol();
    let incompatible = node_with_protocol(
        "compat-node-x",
        "major-2",
        2553,
        ClusterProtocol::exact(incompatible_protocol.version()),
    );
    let entity_type = EntityType::new("CompatRejected");
    let config = ShardingConfig::new(8).expect("valid sharding config");
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let mut runtime =
        ClusterShardingRuntime::new(membership_with_up_nodes(local.clone(), [local.clone()]));
    let region = ShardRegion::new(
        entity_type.clone(),
        config,
        RecordingLocalRoute::new(local.id().clone(), delivered),
    );

    runtime
        .register_region(region)
        .expect("local-only region should register");
    let initial_snapshot = runtime
        .coordinator(&entity_type)
        .expect("coordinator should exist")
        .snapshot();
    assert_eq!(initial_snapshot.owned_shard_count(local.id()), 8);

    let error = runtime
        .apply_discovery(DiscoverySnapshot::new(
            "compatibility-matrix",
            2,
            [local.clone(), incompatible.clone()],
        ))
        .expect_err("incompatible node should be rejected before ownership refresh");
    let ClusterShardingError::Cluster {
        error: ClusterError::IncompatibleNode { node_id, .. },
    } = &error
    else {
        panic!("expected incompatible-node error, got {error:?}");
    };
    assert_eq!(node_id, incompatible.id());

    let rejected_snapshot = runtime
        .coordinator(&entity_type)
        .expect("coordinator should still exist")
        .snapshot();
    assert_eq!(
        rejected_snapshot.owned_shard_count(incompatible.id()),
        0,
        "incompatible node must not acquire shard ownership"
    );
    assert_eq!(rejected_snapshot.owned_shard_count(local.id()), 8);

    assert_readiness_reports_compatibility_failure(local.id().clone(), error.to_string());
}

#[test]
fn http_grpc_and_manifest_versions_match_v1_fixture() {
    let fixture = V1CompatibilityFixture::new();

    assert_eq!(V1_HTTP_API_VERSION, fixture.generated_api_version());
    assert_eq!(
        V1_GRPC_GENERATED_API_VERSION,
        fixture.generated_api_version()
    );
    assert!(V1_HTTP_API_COMPATIBILITY.contains("N/N+1"));
    assert!(V1_GRPC_PROTOBUF_COMPATIBILITY.contains("N/N+1"));

    for expected in [
        "RAKKA_PROTOCOL_VERSION: \"1.0\"",
        "RAKKA_COMPAT_MIN: \"1.0\"",
        "RAKKA_COMPAT_MAX: \"1.1\"",
        "RAKKA_COMPAT_POLICY: n-to-n-plus-one",
        "RAKKA_MANIFEST_VERSION: \"1.0\"",
        "RAKKA_GENERATED_API_VERSION: \"1.0\"",
        "rakka.rs/protocol-version: \"1.0\"",
        "rakka.rs/compatible-min: \"1.0\"",
        "rakka.rs/compatible-max: \"1.1\"",
        "rakka.rs/manifest-version: \"1.0\"",
        "rakka.rs/generated-api-version: \"1.0\"",
    ] {
        assert!(MANIFEST.contains(expected), "manifest missing {expected}");
    }
}

#[test]
fn optional_multi_process_compatibility_example_is_gated() {
    if std::env::var("RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!(
            "skipping multi-process compatibility example; set RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1"
        );
        return;
    }

    let output = Command::new("cargo")
        .args([
            "run",
            "-p",
            "rakka-example-multi-node-sharding",
            "--",
            "--networked-processes",
        ])
        .current_dir(repo_root())
        .output()
        .expect("multi-process compatibility example should run when enabled");

    assert!(
        output.status.success(),
        "multi-process compatibility example failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("launched two node processes"),
        "expected multi-process marker in stdout:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn optional_multi_pod_agent_fault_harness_is_gated() {
    // The agent domain's own multi-process gate, added by slice 6.1. The
    // compatibility example above proves two nodes can talk; this one proves
    // the durable agent entities recover on a *different pod* when the one
    // holding them dies, which is what specification 15 actually requires.
    if std::env::var("RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY")
        .ok()
        .as_deref()
        != Some("1")
    {
        eprintln!(
            "skipping the multi-pod agent fault harness; set \
             RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1"
        );
        return;
    }

    let output = Command::new("cargo")
        .args(["run", "-p", "rakka-example-multi-pod-agent-fault-soak"])
        .current_dir(repo_root())
        .output()
        .expect("the multi-pod agent fault harness should run when enabled");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "multi-pod agent fault harness failed:\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("converged from the shared record")
            || stdout.contains("skipped: loopback binding is unavailable"),
        "expected the sweep marker or an explicit skip in stdout:\n{stdout}"
    );
}

fn assert_readiness_reports_compatibility_failure(local_node_id: NodeId, message: String) {
    let health = KubernetesNodeHealth::new(local_node_id);
    health.record_compatibility_failure(message);

    let readiness = health.readiness_probe();
    assert!(!readiness.passed());
    assert!(
        readiness
            .reasons()
            .iter()
            .any(|reason| reason == "compatibility-not-accepted"),
        "readiness should include compatibility-not-accepted, got {:?}",
        readiness.reasons()
    );

    let recorder = InMemoryMetricsRecorder::new();
    health.record_metrics(&recorder);
    let snapshot = recorder.snapshot();
    let readiness = expect_metric_observation(&snapshot, METRIC_K8S_READINESS, MetricKind::Gauge);
    assert_eq!(readiness.value(), 0.0);
    assert_metric_attribute(&readiness, "outcome", "not-ready");

    let compatibility =
        expect_metric_observation(&snapshot, METRIC_K8S_COMPATIBILITY, MetricKind::Gauge);
    assert_eq!(compatibility.value(), 0.0);
    assert_metric_attribute(&compatibility, "state", "rejected");
}

fn compatibility_registry(
    policy: rakka_remote::SchemaCompatibilityPolicy,
    fixture: &V1CompatibilityFixture,
) -> SerializationRegistry {
    let mut registry = SerializationRegistry::new();
    registry
        .register_protobuf_compatible::<CompatCommand>(
            COMPAT_MESSAGE_TYPE,
            fixture.current_schema_version(),
            policy,
        )
        .expect("compatibility command codec should register");
    registry
}

fn encode_command(action: &str) -> Vec<u8> {
    let message = CompatCommand {
        action: action.to_string(),
    };
    let mut payload = Vec::with_capacity(message.encoded_len());
    message
        .encode(&mut payload)
        .expect("compat command should encode");
    payload
}

fn node_with_protocol(
    logical_id: &str,
    incarnation: &str,
    port: u16,
    protocol: ClusterProtocol,
) -> ClusterNode {
    ClusterNode::new(
        NodeId::new(logical_id, incarnation),
        NodeAddress::new(format!("{logical_id}.rakka.default.svc"), port),
    )
    .with_role("sharded-entity")
    .with_protocol(protocol)
}

fn membership_config() -> MembershipConfig {
    MembershipConfig::new(1, Duration::from_millis(50), Duration::from_millis(100))
}

fn membership_with_up_nodes(
    local: ClusterNode,
    nodes: impl IntoIterator<Item = ClusterNode>,
) -> ClusterMembership {
    let mut membership = ClusterMembership::new(local, membership_config());
    membership
        .record_discovery(DiscoverySnapshot::new("compatibility-matrix", 1, nodes))
        .expect("nodes should be protocol-compatible");
    for node_id in membership
        .snapshot()
        .members()
        .iter()
        .map(|member| member.node().id().clone())
        .collect::<Vec<_>>()
    {
        membership
            .mark_up(&node_id, 2)
            .expect("member should promote to up");
    }
    membership
}

fn entity_owned_by(coordinator: &ShardCoordinator, logical_id: &str) -> EntityId {
    (0..4096)
        .map(|index| EntityId::new(format!("compat-{index}")))
        .find(|entity_id| {
            coordinator
                .owner_for_entity(entity_id)
                .is_ok_and(|owner| owner.logical_id() == logical_id)
        })
        .expect("expected at least one entity to map to requested owner")
}

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rakka-testkit crate should live below workspace root")
}
