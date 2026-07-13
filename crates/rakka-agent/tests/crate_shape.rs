//! Crate shape and feature-gate wiring for `rakka-agent`.
//!
//! These checks guard the boundaries of specification sections 10.1 and 19:
//! the `rig` feature stays default and optional, the crate keeps a
//! `--no-default-features` configuration that the workspace validation script
//! actually exercises, the `rakka` facade keeps its passthrough features, and
//! the agent domain does not reach into the A2A adapter crate.

use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rakka-agent manifest should live under crates/rakka-agent")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

#[test]
fn rig_is_a_default_feature_and_otel_is_opt_in() {
    let manifest = read("crates/rakka-agent/Cargo.toml");

    assert!(
        manifest.contains("default = [\"rig\"]"),
        "the rig feature should be enabled by default"
    );
    assert!(
        manifest.contains("\nrig = ["),
        "the rig feature should be declared"
    );
    assert!(
        manifest.contains("\notel = ["),
        "the otel feature should be declared"
    );
}

#[test]
fn no_default_features_configuration_is_validated_by_the_workspace() {
    let validate = read("scripts/validate.sh");

    for expected in [
        "cargo check -p rakka-agent --no-default-features",
        "cargo test -p rakka-agent --no-default-features",
    ] {
        assert!(
            validate.contains(expected),
            "scripts/validate.sh should run `{expected}`"
        );
    }
}

#[test]
fn facade_propagates_the_agent_features() {
    let facade = read("crates/rakka/Cargo.toml");

    for expected in [
        "agent = [\"dep:rakka-agent\"",
        "agent-rig = [\"agent\", \"rakka-agent?/rig\"]",
        "agent-otel = [\"agent\", \"rakka-agent?/otel\"]",
    ] {
        assert!(
            facade.contains(expected),
            "the rakka facade manifest should contain {expected:?}"
        );
    }

    // The facade turns the crate's default `rig` feature off so that the
    // passthrough is a real knob: an application can take the agent runtime
    // without the Rig dependency.
    assert!(
        facade.contains(
            "rakka-agent = { path = \"../rakka-agent\", version = \"0.1.0\", optional = true, \
             default-features = false }"
        ),
        "the facade should depend on rakka-agent with default features disabled"
    );
}

#[test]
fn agent_domain_does_not_depend_on_the_a2a_adapter() {
    let manifest = read("crates/rakka-agent/Cargo.toml");

    assert!(
        !manifest.contains("rakka-a2a"),
        "the A2A adaptation belongs in rakka-a2a behind its own agents feature"
    );
    assert!(
        manifest.contains("rakka-agent-workflow = { path = \"../rakka-agent-workflow\""),
        "the agent domain builds on the rakka-agent-workflow durable substrate"
    );
}

#[test]
fn every_module_file_is_declared_in_the_module_map() {
    let lib = read("crates/rakka-agent/src/lib.rs");
    let source_dir = repo_root().join("crates/rakka-agent/src");

    let entries = fs::read_dir(&source_dir).expect("failed to read the rakka-agent source dir");
    for entry in entries {
        let path = entry
            .expect("failed to read a rakka-agent source entry")
            .path();
        let Some(module) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        if module == "lib" {
            continue;
        }

        assert!(
            lib.contains(&format!("pub mod {module};")),
            "src/{module}.rs is not declared in the lib.rs module map"
        );
    }
}
