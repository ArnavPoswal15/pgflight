//! Phase 5 acceptance script presence and optional Docker e2e.

use std::path::PathBuf;
use std::process::Command;

fn script_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/accept-phase5.sh")
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[test]
fn accept_phase5_script_exists() {
    let path = script_path();
    assert!(
        path.is_file(),
        "missing scripts/accept-phase5.sh at {}",
        path.display()
    );
}

#[test]
fn accept_phase5_end_to_end() {
    if !docker_available() {
        eprintln!("skip accept-phase5 e2e: docker unavailable");
        return;
    }
    let path = script_path();
    let status = Command::new("bash")
        .arg(&path)
        .status()
        .expect("run accept-phase5.sh");
    assert!(
        status.success(),
        "accept-phase5.sh failed with {:?}",
        status.code()
    );
}
