//! The crate's manifest holds the pin and the three conditions' feature
//! shape: rmcp at exactly 3.4.0 with the client transport features and
//! nothing that enables an environment-proxy egress bypass; the child
//! process transport only behind `child-process`, off by default; the
//! server side only for the testkit.

fn manifest() -> String {
    std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")).expect("manifest")
}

fn dependency_line(manifest: &str, name: &str) -> String {
    manifest
        .lines()
        .find(|line| line.trim_start().starts_with(&format!("{name} ")))
        .unwrap_or_else(|| panic!("{name} is declared"))
        .to_string()
}

#[test]
fn rmcp_is_pinned_exactly_with_the_client_features_and_nothing_proxying() {
    let manifest = manifest();
    let line = dependency_line(&manifest, "rmcp");
    assert!(line.contains("\"=3.4.0\""), "{line}");
    assert!(line.contains("default-features = false"), "{line}");
    for feature in [
        "\"client\"",
        "\"transport-streamable-http-client-reqwest\"",
        "\"reqwest\"",
        "\"transport-io\"",
    ] {
        assert!(line.contains(feature), "{feature} missing: {line}");
    }
    for forbidden in [
        "\"transport-child-process\"",
        "\"server\"",
        "\"reqwest-native-tls\"",
        "system-proxy",
    ] {
        assert!(
            !line.contains(forbidden),
            "{forbidden} must not ride the default dependency: {line}"
        );
    }
}

#[test]
fn the_testkit_reqwest_line_carries_no_default_and_no_proxy() {
    // The crate names `reqwest` only because rmcp does not re-export it, and
    // only for the testkit's `ReqwestClient`. Its defaults would put an
    // environment-proxy egress bypass (and a second TLS stack) on this graph,
    // so the same guard the rmcp line carries is held on this one.
    let manifest = manifest();
    let line = dependency_line(&manifest, "reqwest");
    assert!(line.contains("default-features = false"), "{line}");
    assert!(line.contains("optional = true"), "{line}");
    for forbidden in ["\"native-tls\"", "system-proxy", "\"default\""] {
        assert!(
            !line.contains(forbidden),
            "{forbidden} must not ride the reqwest dependency: {line}"
        );
    }
}

#[test]
fn the_child_process_transport_is_a_non_default_feature() {
    let manifest = manifest();
    let features = manifest
        .split("[features]")
        .nth(1)
        .and_then(|s| s.split("\n[").next())
        .expect("features table");
    assert!(features.contains("default = []"), "{features}");
    assert!(
        features.contains("child-process = [\"rmcp/transport-child-process\"]"),
        "{features}"
    );
    assert!(
        features.contains("testkit = [\"rmcp/server\", \"rmcp/transport-streamable-http-server\""),
        "{features}"
    );
}

#[test]
fn the_crate_is_workspace_only_and_lints_from_the_workspace() {
    let manifest = manifest();
    assert!(manifest.contains("publish = false"));
    assert!(manifest.contains("[lints]\nworkspace = true"));
}
