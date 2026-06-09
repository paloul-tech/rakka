# Rakka Kubernetes Example

This directory contains a reviewable Kubernetes example for running three Rakka nodes with stable pod DNS, readiness/liveness probes, and graceful pre-stop drain.

## Files

- `rakka-node.yaml`: namespace, config, headless internal service, public service, PodDisruptionBudget, and StatefulSet.
- `local-cluster-scenario.sh`: optional kind/minikube scenario runner for applying the manifest, waiting for readiness, exercising drain, and validating a rolling update.

## Ports

- `2552` named `remoting`: internal Rakka remoting and sharding traffic.
- `8080` named `http`: public HTTP adapter, readiness, liveness, and drain endpoints.
- `50051` named `grpc`: public gRPC adapter.

## Kubernetes Services

`rakka-internal` is a headless service with `clusterIP: None`. It gives pods stable DNS names such as:

```text
rakka-node-0.rakka-internal.rakka-system.svc.cluster.local
```

That shape matches `KubernetesDnsDiscoveryConfig::new("rakka-system", "rakka-internal", 2552)`.

`rakka-public` is a normal `ClusterIP` service for HTTP and gRPC ingress inside the cluster.

## Health And Drain

The example expects the application image to expose:

- `GET /ready`: returns success after cluster join, compatibility acceptance, and required service registration.
- `GET /live`: stays healthy during ordinary shard rebalance and graceful drain, but fails for stuck runtime conditions.
- `GET /drain`: starts the Slice 5G drain controller and returns a drain report.

The StatefulSet pre-stop hook calls `/drain`, and `terminationGracePeriodSeconds` gives the node time to mark itself draining, hand off shards, drain streams, stop process actors, and leave membership.

## Rolling Compatibility

The manifest records an N/N+1 compatibility window:

```text
RAKKA_PROTOCOL_VERSION=1.0
RAKKA_COMPAT_MIN=1.0
RAKKA_COMPAT_MAX=1.1
RAKKA_COMPAT_POLICY=n-to-n-plus-one
```

During rolling updates, the next image should remain compatible with the current minor version until all pods have rolled.

## Local Cluster Scenario

The scenario script is intentionally gated because it talks to the active Kubernetes context.

Dry run:

```sh
RAKKA_K8S_SCENARIO_DRY_RUN=1 examples/kubernetes/local-cluster-scenario.sh
```

Run against the current context:

```sh
RAKKA_K8S_IMAGE=your-registry/rakka-node:dev examples/kubernetes/local-cluster-scenario.sh
```

The script applies a temporary manifest with the provided image, waits for three pods, checks probes, calls drain on `rakka-node-1`, deletes that pod to exercise graceful replacement, and optionally performs a rolling update when `RAKKA_K8S_NEXT_IMAGE` is set.

For the probe checks, the image should include either `wget` or `curl`. Kubernetes itself uses native HTTP probes and does not require either tool for readiness, liveness, or pre-stop drain.

The app-specific shard-routing request is left to Slice 5J examples because this slice only defines the Kubernetes wiring.
