# Rakka V1 Security and Operational Defaults

Slice V1H makes the default trust boundaries explicit. Rakka v1 still expects applications and operators to make deployment decisions, but the framework defaults are conservative and fail closed where the runtime owns the boundary.

## Trust Boundaries

Rakka internal remoting is trusted cluster traffic. It is not a public client API.

External clients should use application HTTP/gRPC adapters or other explicitly configured public protocols. Internal remote envelopes are intended for Rakka nodes that have already been discovered, admitted by protocol compatibility checks, and registered as known peers.

Rakka protects:

- typed actor/entity delivery inside one Rakka node;
- remote envelope decode and schema policy checks;
- known-peer TCP remoting admission;
- N/N+1 protocol compatibility checks during rolling updates;
- process actor executable allowlists and environment clearing by default;
- Kubernetes readiness/drain hooks that mark nodes unavailable before shutdown work starts.

Kubernetes and the operator must protect:

- network access to the internal remoting service;
- ingress, authentication, authorization, and TLS/mTLS;
- pod security context, filesystem permissions, service accounts, and secrets;
- image provenance and executable contents;
- resource limits and disruption budgets.

Applications must protect:

- public HTTP/gRPC authorization and request validation;
- application-level idempotency, retries, and durable workflow semantics;
- which child binaries are allowed and which environment variables they receive;
- route labels and observability cardinality.

## Security Profiles

`rakka_core::SecurityDefaults` exposes three reviewable profiles:

| Profile | Remoting bind | Public adapter bind | Intended use |
| --- | --- | --- | --- |
| `development` | `127.0.0.1` | `127.0.0.1` | Single-machine development. |
| `local-cluster` | `0.0.0.0` | `0.0.0.0` | kind/minikube pod networking. |
| `production-like` | `0.0.0.0` | `0.0.0.0` | Kubernetes deployment with NetworkPolicy or equivalent controls. |

All profiles keep these defaults:

- remoting requires registered peers;
- remoting is not a public API;
- process execution requires an executable allowlist;
- process actors do not inherit the node environment by default.

## Remoting Defaults

`TcpRemoteTransportConfig::default()` is local-development safe:

- bind address: `127.0.0.1:2552`;
- outbound queue capacity per peer: `1024`;
- connect timeout: `2s`;
- reconnect backoff: `100ms`;
- idle timeout: `30s`;
- max frame size: `16MiB`;
- peer registration required: `true`.

For Kubernetes, bind the listener inside pod networking, but keep peer admission tied to discovery:

```text
RAKKA_REMOTING_BIND_ADDR=0.0.0.0:2552
RAKKA_REMOTING_TRUST_BOUNDARY=trusted-cluster
RAKKA_REMOTING_ALLOWED_PEERS=discovery
```

Unknown peers, unexpected peer ids, incompatible protocol handshakes, malformed frames, and decode failures fail closed with typed errors and metrics.

## Process Defaults

`ProcessSpec::new` is conservative:

- program path must be absolute;
- executable must match an explicit `ExecutableAllowlist`;
- environment inheritance is disabled;
- stdin, stdout, and stderr default to `Null`;
- graceful shutdown defaults to closing stdin;
- startup timeout defaults to `5s`;
- shutdown timeout defaults to `5s`;
- relative working directories are rejected.

Use explicit environment declarations for the small set of variables a child needs:

```rust
let spec = ProcessSpec::new("/opt/rakka/bin/legacy-worker")
    .env("RAKKA_WORKER_MODE", "batch")
    .stdin(ProcessStdio::Piped)
    .stdout(ProcessStdio::Piped);
```

Only use `inherit_environment()` for tightly controlled binaries where inherited secrets are intended.

## Operational Timeouts

`rakka_core::OperationalTimeoutDefaults` records the v1 default timeout budget:

| Boundary | Default |
| --- | --- |
| Actor ask | `5s` |
| Remote connect | `2s` |
| Remote idle | `30s` |
| Stream drain | `5s` |
| Process startup | `5s` |
| Process shutdown | `5s` |
| Kubernetes pre-stop drain | `30s` |
| Kubernetes termination grace period | `45s` |

The Kubernetes pre-stop drain budget is intentionally shorter than the termination grace period, leaving room for reporting and final cleanup.

## Kubernetes Example Defaults

The Kubernetes manifest exposes the profile and timeout defaults as environment variables:

```text
RAKKA_DEPLOYMENT_PROFILE=production-like
RAKKA_REMOTING_TRUST_BOUNDARY=trusted-cluster
RAKKA_REMOTING_ALLOWED_PEERS=discovery
RAKKA_PROCESS_ALLOWLIST_REQUIRED=true
RAKKA_PROCESS_INHERIT_ENVIRONMENT=false
RAKKA_ACTOR_ASK_TIMEOUT_MS=5000
RAKKA_REMOTE_CONNECT_TIMEOUT_MS=2000
RAKKA_REMOTE_IDLE_TIMEOUT_MS=30000
RAKKA_STREAM_DRAIN_TIMEOUT_MS=5000
RAKKA_PROCESS_STARTUP_TIMEOUT_MS=5000
RAKKA_PROCESS_SHUTDOWN_TIMEOUT_MS=5000
RAKKA_K8S_PRESTOP_TIMEOUT_MS=30000
```

These values are examples for application config parsing; Rakka crates still expose typed constructors so applications can choose stricter values.

## Validation

Run focused tests:

```sh
cargo test -p rakka-core --test security_operational_defaults
cargo test -p rakka-remote --test remote_boundary tcp_remote_defaults_are_loopback_bounded_and_known_peer_only
cargo test -p rakka-process --test process_lifecycle process_spec_defaults_are_conservative
cargo test -p rakka-k8s --test health_drain kubernetes_timeout_defaults_leave_room_for_prestop_cleanup
cargo test -p rakka-k8s --test kubernetes_manifests
```
