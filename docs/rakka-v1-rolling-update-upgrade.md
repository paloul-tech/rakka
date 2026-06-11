# Rakka V1 Rolling Update Upgrade Note

This note turns the compatibility policy into an operator sequence for N/N+1 Kubernetes updates.

Use it with `docs/rakka-compatibility.md`, `docs/rakka-v1-security-operational-defaults.md`, and `examples/kubernetes/README.md`.

## Supported Upgrade Shape

Rakka v1 supports one rolling-update window at a time:

- release N and release N+1 may coexist when their cluster protocol ranges overlap;
- message schema changes must be additive inside the window;
- manifests and generated API metadata must describe the same compatibility window;
- incompatible nodes must fail readiness and must not acquire shard ownership.

Rakka v1 does not support arbitrary multi-version clusters.

## Preflight Checklist

Before rollout:

1. Confirm both images use the expected Rakka crate/package version.
2. Confirm `RAKKA_PROTOCOL_VERSION`, `RAKKA_COMPAT_MIN`, and `RAKKA_COMPAT_MAX` describe the N/N+1 window.
3. Confirm Protobuf schema changes are additive: no field-number reuse, type change, removal, or semantic change.
4. Confirm the serialization registry accepts the old and new schema versions intentionally.
5. Confirm the manifest version and generated API version are correct.
6. Confirm readiness, liveness, drain, metrics, snapshots, public HTTP/gRPC, and internal remoting routes are exposed by the application image.
7. Confirm the PodDisruptionBudget allows only safe disruption for the workload.

Required local checks:

```sh
scripts/validate.sh
scripts/package-check.sh
```

Compatibility-focused checks:

```sh
cargo test -p rakka-testkit --test compatibility_matrix -- --nocapture
cargo test -p rakka-k8s --test kubernetes_manifests
```

Optional gated checks:

```sh
RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 cargo test -p rakka-testkit --test compatibility_matrix optional_multi_process_compatibility_example_is_gated -- --nocapture
RAKKA_K8S_RUN_LOCAL_CLUSTER=1 RAKKA_K8S_IMAGE=<image-n> RAKKA_K8S_NEXT_IMAGE=<image-n-plus-one> examples/kubernetes/local-cluster-scenario.sh
```

## Rollout Sequence

1. Deploy release N with compatibility metadata that admits N+1.
2. Build release N+1 with additive schemas and matching compatibility metadata.
3. Start a partitioned rollout or one-pod canary if the platform supports it.
4. Wait for the first N+1 pod to become ready.
5. Verify membership accepts the N+1 node.
6. Verify remote entity routing works across old and new nodes.
7. Verify metrics and snapshots report expected membership, remoting, and shard state.
8. Continue rolling one pod at a time.
9. During each pod termination, call drain or let the pre-stop hook call drain.
10. Confirm readiness fails during drain and recovers on the replacement pod.
11. Complete the rollout only after every pod is on N+1 and routing remains healthy.

After all pods are on N+1, the next release may advance the compatibility window to N+1/N+2.

## Rollback Sequence

Rollback is safe only while N remains inside the advertised compatibility window.

1. Stop advancing the rollout.
2. Keep existing N pods ready.
3. Replace unhealthy N+1 pods with N images.
4. Confirm incompatible or unhealthy nodes fail readiness rather than joining ownership.
5. Re-run compatibility and routing checks.
6. Do not deploy incompatible schema changes until the cluster is fully drained or bridged.

## Incompatible Changes

Use an exact-version policy, a bridge release, a separate cluster, or a full drain for:

- removed Protobuf fields used by older nodes;
- field-number reuse;
- changed field type or semantics;
- required interpretation changes where old defaults are unsafe;
- remote envelope version breaks;
- cluster protocol major-version breaks.

Rakka should fail closed instead of trying best-effort delivery across these changes.

## Observable Signals

During rollout, watch:

- readiness reasons, especially `compatibility-not-accepted`;
- `rakka.k8s.compatibility` metrics;
- `rakka.k8s.readiness` metrics;
- remoting connection states and failures;
- shard ownership revision and owner distribution;
- process-backed entity handoff events if the workload uses process actors.

The local-cluster scenario exercises this shape in a gated environment.
