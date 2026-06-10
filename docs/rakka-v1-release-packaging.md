# Rakka V1 Release Packaging

This document defines the repeatable local and CI checks for a reviewable Rakka v1 release candidate. It does not publish crates or images; it keeps the repository ready for that decision.

## No Publishing Without Explicit Approval

Do not publish any Rakka crate, package, release artifact, container image, or generated bundle to a public or private registry from this repository unless the user gives explicit approval for that specific publishing action in the same conversation turn.

Validation commands such as `cargo package`, `cargo package --list`, and `scripts/package-check.sh` are allowed only as local packaging checks. They must not be replaced with `cargo publish`, registry upload commands, GitHub release publishing, image pushes, or any other public upload path without explicit user approval.

`scripts/package-check.sh` must always run `cargo package` in offline mode. If the local Cargo cache is missing dependencies or index metadata, the package check should fail locally rather than accessing crates.io.

This policy applies even when release-candidate docs, CI, or package metadata are being prepared. Release readiness is not permission to publish.

## Required Local Validation

Run these from the workspace root:

```sh
scripts/validate.sh
scripts/package-check.sh
```

`scripts/validate.sh` runs format, clippy, the full workspace test suite, minimal feature checks for stream and process crates, rustdoc, and the safe Kubernetes scenario dry run.

`scripts/package-check.sh` runs offline `cargo package --list` checks for publishable crates, verifies that `rakka-core` can be fully packaged without `--no-verify`, and confirms every example crate is excluded from publishing.

Crates with unpublished internal Rakka dependencies are package-list checked offline rather than fully packaged. Cargo full package generation for those crates requires resolving versioned internal dependencies from a registry, which is intentionally not allowed by this script.

## Required CI Jobs

Required CI jobs are safe for normal pull requests and pushes:

- `Required Validation`: calls `scripts/validate.sh` on stable Rust.
- `MSRV Check`: runs `cargo check --workspace --all-targets` on Rust `1.80.0`.
- `Offline Package Check`: calls `scripts/package-check.sh`.
- `Kubernetes Scenario Dry Run`: runs the Kubernetes scenario in dry-run mode without cluster access.

## Optional CI Jobs

Optional jobs are gated by `workflow_dispatch` inputs because they need external services or a mutable cluster:

- `PostgreSQL Integration`: runs `cargo test -p rakka-persistence-postgres` with `RAKKA_POSTGRES_TEST_DSN`.
- `Local Kubernetes Cluster`: runs the gated local-cluster contract test with `RAKKA_K8S_RUN_LOCAL_CLUSTER=1` and image variables supplied by the repository or operator.

## Publishable Crates

The publishable crate set is:

- `rakka-core`
- `rakka-persistence`
- `rakka-persistence-postgres`
- `rakka-remote`
- `rakka-cluster`
- `rakka-sharding`
- `rakka-workflow`
- `rakka-stream`
- `rakka-process`
- `rakka-http`
- `rakka-grpc`
- `rakka-k8s`
- `rakka-testkit`

Examples are workspace packages for review and testing, but they must remain `publish = false`.

Internal crate dependencies use both `path` and `version = "0.1.0"`. The local package script uses offline `--list` checks for crates that depend on other unpublished Rakka crates, then verifies `rakka-core` fully because it has no internal unpublished dependency.

## Release Profile Guidance

The workspace keeps normal Cargo debug and release profiles for now. Application images should build their own release binaries with:

```sh
cargo build --release --locked -p <application-package>
```

Operators should keep separate image tags for N and N+1 rolling updates, and should not reuse mutable tags for compatibility validation.

## Image Build Notes

Rakka does not ship a production application image in this repository. A Rakka node image used with `examples/kubernetes/rakka-node.yaml` must provide an application binary that exposes:

- internal Rakka TCP remoting on port `2552`
- public HTTP on port `8080`
- public gRPC on port `50051`
- readiness and liveness routes
- drain route
- Prometheus metrics route
- OpenTelemetry bridge route
- operational snapshot route
- remote-sharding scenario route used by the local-cluster example

Production-like images should run as a non-root user, bind only the required ports, inherit no unnecessary environment, and set process actor executable allowlists explicitly.

## Release Candidate Checklist

Before tagging a v1 release candidate:

1. Run `scripts/validate.sh`.
2. Run `scripts/package-check.sh`.
3. Run or explicitly defer PostgreSQL validation.
4. Run or explicitly defer local Kubernetes validation.
5. Review `CHANGELOG.md`.
6. Review README and docs links.
7. Confirm `rust-toolchain.toml` and workspace `rust-version` agree.

Actual `cargo publish` and image registry publishing remain out of scope for this slice.
