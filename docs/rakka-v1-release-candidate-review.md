# Rakka V1 Release Candidate Review

This is the final review packet for the v1 release candidate. It links the implemented behavior docs, validation commands, example coverage, known limitations, and remaining review questions.

Release readiness is not permission to publish crates, images, generated bundles, or release artifacts. Publishing requires explicit approval for that exact action.

## Required Validation

Run from a clean checkout:

```sh
scripts/validate.sh
scripts/package-check.sh
```

Expected required results:

- `scripts/validate.sh` exits `0` after format, clippy, workspace tests, minimal feature checks, docs, and Kubernetes dry-run validation.
- `scripts/package-check.sh` exits `0` while using Cargo offline mode only.
- Cargo may warn that manifests have no license metadata. That warning is expected until the repository declares a license.
- No command publishes crates, pushes images, creates releases, or uploads artifacts.

Last local V1J verification on June 10, 2026:

- `cargo fmt --all -- --check`: passed.
- `cargo test -p rakka-testkit --test repository_hygiene`: passed.
- `scripts/package-check.sh`: passed in offline mode with expected missing-license metadata warnings.
- `scripts/validate.sh`: passed outside the sandbox. The sandboxed run failed only because the generated-contracts integration test needed local process/loopback permissions.

## Optional Gated Validation

These checks require local services or mutable infrastructure:

```sh
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-persistence-postgres
RAKKA_POSTGRES_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres cargo test -p rakka-sharding-postgres
RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1 cargo test -p rakka-testkit --test compatibility_matrix optional_multi_process_compatibility_example_is_gated -- --nocapture
RAKKA_K8S_RUN_LOCAL_CLUSTER=1 RAKKA_K8S_IMAGE=<image> RAKKA_K8S_NEXT_IMAGE=<image-next> examples/kubernetes/local-cluster-scenario.sh
```

Optional checks may be deferred, but the deferral should be visible in release-candidate notes.

## Product Docs

- `docs/rakka-actor-framework-spec.md`: original actor framework spec modeled against Akka.
- `docs/rakka-phase-3-remote-sharding.md`: remote entity routing foundations.
- `docs/rakka-phase-4-process-workflow.md`: process actor and durable workflow foundations.
- `docs/rakka-phase-5-integration-surfaces.md`: stream, HTTP, gRPC, Kubernetes, and metrics integration foundations.
- `docs/rakka-compatibility.md`: N/N+1 compatibility policy.
- `docs/rakka-api-boundary-inventory.md`: facade, foundation, adapter, and test/support API boundaries.
- `docs/rakka-akka-parity-migration-notes.md`: migration notes toward the Akka-like facade APIs.
- `docs/rakka-v1-api-review.md`: public API and crate-boundary review.
- `docs/rakka-v1-generated-contracts.md`: generated gRPC contracts and mirrored HTTP routes.
- `docs/rakka-v1-observability-exporters.md`: Prometheus/OpenTelemetry adapters and snapshots.
- `docs/rakka-v1-security-operational-defaults.md`: security defaults and operator responsibilities.
- `docs/rakka-v1-release-packaging.md`: CI, offline package checks, and release-candidate packaging.
- `docs/rakka-v1-reliability-boundaries.md`: reliability guarantees and non-guarantees.
- `docs/rakka-v1-rolling-update-upgrade.md`: N/N+1 rolling-update sequence.
- `docs/rakka-v1-known-limitations-roadmap.md`: known limitations and post-v1 roadmap.

Historical and active implementation plans live under `docs/plans/`.

## Example Coverage

| Example | Coverage |
| --- | --- |
| `rakka-example-minimal-system` | typed actor startup and ask/reply. |
| `rakka-example-durable-counter` | durable state recovery. |
| `rakka-example-durable-workflow` | durable inbox/outbox, deduplication, retry, and recovery. |
| `rakka-example-multi-node-sharding` | deterministic sharding, TCP loopback sharding, and multi-process sharding. |
| `rakka-example-external-binary-wrapper` | line-json child process ownership. |
| `rakka-example-edge-gateway` | HTTP/gRPC adapters, streams, process-backed service, Kubernetes health/drain, and metrics. |
| `rakka-example-generated-contracts` | generated tonic services, mirrored HTTP routes, workflow command, and process-backed service. |
| `examples/kubernetes` | StatefulSet, remoting service, public service, readiness/liveness, drain, metrics, snapshots, and gated local-cluster scenario. |

## Review Checklist

- Required validation commands pass locally.
- Optional gated checks are either run or explicitly deferred.
- README points to current product docs and validation commands.
- Plan files live under `docs/plans/`, not mixed into the product-doc index.
- Compatibility docs define the N/N+1 window and failure behavior.
- Reliability boundaries document at-most-once actor delivery and opt-in workflow reliability.
- Security docs state the trusted-cluster remoting boundary.
- Release packaging docs state offline-only package checks and the no-publishing policy.
- Known limitations include missing repository license declaration.
- Changelog contains release-candidate notes and validation expectations.

## Release Candidate Decision

After review, the remaining decision is not technical packaging readiness alone. A public release also needs an explicit repository license, contribution policy, and a user-approved publishing action.
