//! Repository hygiene checks for v1 release-candidate packaging.

use std::fs;
use std::path::{Path, PathBuf};

const PUBLISHABLE_CRATES: &[&str] = &[
    "rakka",
    "rakka-core",
    "rakka-persistence",
    "rakka-persistence-postgres",
    "rakka-remote",
    "rakka-cluster",
    "rakka-sharding",
    "rakka-sharding-postgres",
    "rakka-workflow",
    "rakka-stream",
    "rakka-process",
    "rakka-http",
    "rakka-grpc",
    "rakka-k8s",
    "rakka-testkit",
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("testkit manifest should live under crates/rakka-testkit")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    fs::read_to_string(repo_root().join(relative))
        .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

fn section_between<'a>(contents: &'a str, start: &str, end: &str) -> &'a str {
    let start_index = contents
        .find(start)
        .unwrap_or_else(|| panic!("missing section start {start:?}"));
    let remaining = &contents[start_index..];
    let end_index = remaining
        .find(end)
        .unwrap_or_else(|| panic!("missing section end {end:?}"));
    &remaining[..end_index]
}

#[test]
fn ci_uses_repository_validation_entrypoints() {
    let ci = read(".github/workflows/ci.yml");

    for expected in [
        "scripts/validate.sh",
        "scripts/package-check.sh",
        "dtolnay/rust-toolchain@1.80.0",
        "RAKKA_POSTGRES_TEST_DSN",
        "RAKKA_K8S_RUN_LOCAL_CLUSTER",
        "workflow_dispatch",
    ] {
        assert!(ci.contains(expected), "CI missing {expected}");
    }
}

#[test]
fn local_scripts_are_documented_and_executable() {
    for script in ["scripts/validate.sh", "scripts/package-check.sh"] {
        let path = repo_root().join(script);
        let contents = read(script);

        assert!(path.exists(), "missing {script}");
        assert!(
            contents.starts_with("#!/usr/bin/env sh"),
            "{script} missing shebang"
        );
        assert!(
            contents.contains("set -eu"),
            "{script} missing fail-fast shell mode"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = fs::metadata(&path)
                .unwrap_or_else(|error| panic!("failed to stat {script}: {error}"))
                .permissions()
                .mode();
            assert_ne!(mode & 0o111, 0, "{script} should be executable");
        }
    }

    let readme = read("README.md");
    assert!(readme.contains("scripts/validate.sh"));
    assert!(readme.contains("scripts/package-check.sh"));

    let package_script = read("scripts/package-check.sh");
    assert!(
        !package_script.contains("cargo publish"),
        "package validation script must never publish crates"
    );
    assert!(
        package_script.contains("must never publish crates"),
        "package validation script should state the no-publish policy"
    );
    assert!(
        package_script.contains("--offline"),
        "package validation script must run cargo package offline"
    );
    assert!(
        package_script.contains("FULL_PACKAGE_CRATES"),
        "package validation script should make full-package crates explicit"
    );

    let release_docs = read("docs/rakka-v1-release-packaging.md");
    assert!(
        release_docs.contains("must always run `cargo package` in offline mode"),
        "release docs should state the offline package-check policy"
    );
    assert!(
        release_docs.contains("package-list checked offline"),
        "release docs should explain unpublished internal dependency handling"
    );
}

#[test]
fn publishable_crates_have_release_metadata_and_versioned_internal_deps() {
    for crate_name in PUBLISHABLE_CRATES {
        let manifest_path = format!("crates/{crate_name}/Cargo.toml");
        let manifest = read(&manifest_path);

        assert!(
            manifest.contains("description.workspace = true"),
            "{manifest_path} should inherit workspace description"
        );

        for line in manifest
            .lines()
            .filter(|line| line.contains("path = \"../rakka-"))
        {
            assert!(
                line.contains("version = \"0.1.0\""),
                "{manifest_path} has an internal path dependency without version: {line}"
            );
        }
    }
}

#[test]
fn facade_prelude_is_curated() {
    let facade = read("crates/rakka/src/lib.rs");
    let prelude = section_between(&facade, "pub mod prelude", "/// Actor runtime primitives.");

    for expected in [
        "pub use rakka_core::{",
        "pub use rakka_persistence::{",
        "pub use rakka_sharding::facade::{",
        "ClusterSharding",
        "EntityTypeKey",
        "ShardedEntityRef",
        "pub use rakka_sharding::{",
        "EntityId",
        "EntityRef",
        "EntityType",
        "ShardingConfig",
        "ShardCoordinatorLease",
        "ShardCoordinatorStore",
        "pub use rakka_stream::{BoundedStream, StreamError, StreamResult, StreamSink, StreamSource};",
    ] {
        assert!(prelude.contains(expected), "prelude missing {expected}");
    }

    for internal in [
        "ShardCoordinator,",
        "ShardRegion",
        "LocalEntityRoute",
        "RemoteEntityRoute",
        "ClusterNodeRuntime",
        "RemoteEnvelope",
        "TcpRemoteTransport",
        "KubernetesDrainReport",
    ] {
        assert!(
            !prelude.contains(internal),
            "prelude should not expose foundation/adapter internals: {internal}"
        );
    }

    let api_inventory = read("docs/rakka-api-boundary-inventory.md");
    assert!(api_inventory.contains("Facade"));
    assert!(api_inventory.contains("Foundation"));
    assert!(api_inventory.contains("Adapter"));
    assert!(api_inventory.contains("Test/support"));

    let migration = read("docs/rakka-akka-parity-migration-notes.md");
    assert!(migration.contains("Phase 0"));
    assert!(migration.contains("rakka::prelude"));
}

#[test]
fn examples_are_workspace_only_packages() {
    let examples_dir = repo_root().join("examples");
    let entries = fs::read_dir(&examples_dir)
        .unwrap_or_else(|error| panic!("failed to read examples dir: {error}"));

    let mut example_manifests = Vec::new();
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| panic!("failed to read examples entry: {error}"));
        let path = entry.path().join("Cargo.toml");
        if path.exists() {
            example_manifests.push(path);
        }
    }

    assert!(
        !example_manifests.is_empty(),
        "expected at least one example manifest"
    );

    for manifest_path in example_manifests {
        let contents = fs::read_to_string(&manifest_path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", manifest_path.display()));
        assert!(
            contents.contains("publish = false"),
            "{} should be excluded from publishing",
            manifest_path.display()
        );
    }
}

#[test]
fn release_docs_and_ignore_rules_are_present() {
    for required in [
        "CHANGELOG.md",
        "docs/rakka-v1-release-packaging.md",
        "docs/rakka-v1-release-candidate-review.md",
        "docs/rakka-v1-reliability-boundaries.md",
        "docs/rakka-v1-rolling-update-upgrade.md",
        "docs/rakka-v1-known-limitations-roadmap.md",
        "rust-toolchain.toml",
    ] {
        assert!(repo_root().join(required).exists(), "missing {required}");
    }

    let root_manifest = read("Cargo.toml");
    assert!(root_manifest.contains("rust-version = \"1.80\""));
    assert!(root_manifest.contains("description = "));

    let release_docs = read("docs/rakka-v1-release-packaging.md");
    assert!(release_docs.contains("No Publishing Without Explicit Approval"));
    assert!(release_docs.contains("Release readiness is not permission to publish"));

    let readme = read("README.md");
    assert!(readme.contains("v1 release-candidate foundation"));
    assert!(readme.contains("docs/rakka-v1-release-candidate-review.md"));
    assert!(readme.contains("docs/rakka-v1-reliability-boundaries.md"));
    assert!(readme.contains("docs/rakka-v1-rolling-update-upgrade.md"));
    assert!(readme.contains("docs/rakka-v1-known-limitations-roadmap.md"));

    let gitignore = read(".gitignore");
    for ignored in ["/target/", ".idea/", "/.env", "/dist/", "*.crate"] {
        assert!(gitignore.contains(ignored), ".gitignore missing {ignored}");
    }
}

#[test]
fn implementation_plans_are_separated_from_product_docs() {
    for plan in [
        "rakka-akka-parity-implementation-plan.md",
        "rakka-phase-3-continuation-plan.md",
        "rakka-phase-4-continuation-plan.md",
        "rakka-phase-5-continuation-plan.md",
        "rakka-v1-implementation-plan.md",
        "rakka-v1-hardening-plan.md",
    ] {
        assert!(
            !repo_root().join("docs").join(plan).exists(),
            "{plan} should live under docs/plans, not docs/"
        );
        assert!(
            repo_root().join("docs/plans").join(plan).exists(),
            "{plan} should exist under docs/plans"
        );
    }

    let plan_index = read("docs/plans/README.md");
    assert!(plan_index.contains("historical and active implementation plans"));
    assert!(plan_index.contains("product docs in `docs/`"));

    let review = read("docs/rakka-v1-release-candidate-review.md");
    assert!(review.contains("Historical and active implementation plans live under `docs/plans/`"));
}
