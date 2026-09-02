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
        "dtolnay/rust-toolchain@",
        "RAKKA_POSTGRES_TEST_DSN",
        "RAKKA_K8S_RUN_LOCAL_CLUSTER",
        "workflow_dispatch",
    ] {
        assert!(ci.contains(expected), "CI missing {expected}");
    }
}

#[test]
fn msrv_references_stay_in_sync_with_workspace_manifest() {
    let manifest = read("Cargo.toml");
    let msrv = manifest
        .lines()
        .find_map(|line| {
            line.strip_prefix("rust-version = \"")
                .and_then(|rest| rest.strip_suffix('"'))
        })
        .expect("workspace manifest should declare rust-version");

    // Historical plan documents are point-in-time records and intentionally
    // excluded; everything here describes the current toolchain requirement.
    for (file, reference) in [
        (
            ".github/workflows/ci.yml",
            format!("dtolnay/rust-toolchain@{msrv}.0"),
        ),
        ("AGENTS.md", format!("MSRV is Rust `{msrv}`")),
        ("CLAUDE.md", format!("MSRV is Rust {msrv}")),
        (
            "docs/rakka-v1-release-packaging.md",
            format!("on Rust `{msrv}.0`"),
        ),
    ] {
        assert!(
            read(file).contains(&reference),
            "{file} should reference the workspace MSRV as {reference:?}"
        );
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
        "pub use rakka_stream::{",
        "AckProtocol",
        "ActorSinkMessage",
        "ActorSourceMessage",
        "Flow",
        "RunnableStream",
        "Sink",
        "Source",
        "StreamRunError",
        "StreamSink",
        "StreamSource",
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
    assert!(root_manifest.contains("rust-version = "));
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

/// Every `crates/*` directory, which must also be the workspace's crate member
/// set.
fn workspace_crates() -> Vec<String> {
    let mut crates: Vec<String> = fs::read_dir(repo_root().join("crates"))
        .expect("crates/ is readable")
        .map(|entry| entry.expect("a directory entry is readable"))
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    crates.sort();
    let mut members: Vec<String> = read("Cargo.toml")
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix("\"crates/")
                .and_then(|rest| rest.strip_suffix("\","))
                .map(str::to_string)
        })
        .collect();
    members.sort();
    assert_eq!(
        crates, members,
        "every crates/ directory must be a workspace member, and vice versa"
    );
    crates
}

#[test]
fn crate_inventories_name_every_workspace_crate() {
    let crates = workspace_crates();
    assert!(
        crates.len() > 1,
        "the crate scan found nothing, so nothing is checked"
    );
    for (document, start, end) in [
        (
            "docs/rakka-api-boundary-inventory.md",
            "## Crate Inventory",
            "## Prelude Inventory",
        ),
        (
            "docs/rakka-v1-api-review.md",
            "## Crate Map",
            "## Feature Boundaries",
        ),
    ] {
        let contents = read(document);
        let table = section_between(&contents, start, end);
        for name in &crates {
            assert!(
                table.contains(&format!("| `{name}` |")),
                "{document} has no crate row for `{name}`"
            );
        }
        for row in table.lines().filter(|line| line.starts_with("| `")) {
            let name = row
                .split('`')
                .nth(1)
                .expect("a crate row names its crate in backticks");
            assert!(
                crates.iter().any(|known| known == name),
                "{document} lists `{name}`, which is not a crate in crates/"
            );
        }
    }
}

/// Every relative link in a Markdown file, as `(link, resolved path)`.
fn relative_links(relative: &str) -> Vec<(String, PathBuf)> {
    let contents = read(relative);
    let directory = repo_root()
        .join(relative)
        .parent()
        .expect("a document has a parent directory")
        .to_path_buf();
    let mut links = Vec::new();
    for chunk in contents.split("](").skip(1) {
        let Some((target, _)) = chunk.split_once(')') else {
            continue;
        };
        let target = target.split_whitespace().next().unwrap_or("");
        let (path, _anchor) = target.split_once('#').unwrap_or((target, ""));
        if path.is_empty() || path.contains("://") || path.starts_with("mailto:") {
            continue;
        }
        links.push((target.to_string(), directory.join(path)));
    }
    links
}

#[test]
fn documentation_relative_links_resolve() {
    let mut documents = vec!["README.md".to_string()];
    let mut docs: Vec<String> = fs::read_dir(repo_root().join("docs"))
        .expect("docs/ is readable")
        .map(|entry| entry.expect("a directory entry is readable").path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("md"))
        .map(|path| format!("docs/{}", path.file_name().unwrap().to_string_lossy()))
        .collect();
    docs.sort();
    documents.extend(docs);

    let mut broken = Vec::new();
    let mut checked = 0;
    for document in &documents {
        for (link, resolved) in relative_links(document) {
            checked += 1;
            if !resolved.exists() {
                broken.push(format!("{document} -> {link}"));
            }
        }
    }
    assert!(
        checked > 0,
        "no relative link was found, so nothing was checked"
    );
    assert!(
        broken.is_empty(),
        "relative links that resolve to nothing:\n{}",
        broken.join("\n")
    );
}

#[test]
fn readme_links_the_agent_documentation_set() {
    let readme = read("README.md");
    for document in [
        "docs/rakka-agents.md",
        "docs/rakka-agent-recovery-scenarios.md",
        "docs/rakka-agent-fault-injection-matrix.md",
        "docs/rakka-agent-security-validation-matrix.md",
        "docs/rakka-agent-telemetry-validation-matrix.md",
        "docs/rakka-agent-observability-catalogue.md",
        "docs/plans/rakka-agent/",
    ] {
        assert!(
            readme.contains(document),
            "README should point at {document}"
        );
        if let Some(file) = document.strip_suffix(".md") {
            assert!(
                repo_root().join(format!("{file}.md")).is_file(),
                "{document} does not exist"
            );
        }
    }
}
