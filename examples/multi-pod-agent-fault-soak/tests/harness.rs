//! The gate: the harness is a real multi-process run, so it is opt-in.
//!
//! `scripts/validate.sh` runs `cargo test --workspace --all-features`, and this
//! harness spawns pod processes and sweeps every durable write of each. It runs
//! only when `RAKKA_RUN_MULTI_PROCESS_COMPATIBILITY=1` is set — the same gate
//! the repository's other multi-process check uses, named by slice 6.1 itself.

use std::process::Command;

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("the example lives two levels below the repository root")
        .to_path_buf()
}

#[test]
fn optional_multi_pod_agent_fault_harness_is_gated() {
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
        .expect("the multi-pod harness should run when enabled");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "the multi-pod harness failed:\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    if stdout.contains("skipped: loopback binding is unavailable") {
        eprintln!("multi-pod harness skipped: loopback binding is unavailable");
        return;
    }
    assert!(
        stdout.contains("converged from the shared record"),
        "expected the sweep's convergence marker in stdout:\n{stdout}"
    );
}
